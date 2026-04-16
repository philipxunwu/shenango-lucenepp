
extern "C"
{
#include <base/time.h>
#include <base/log.h>
#include <net/ip.h>
#include <runtime/smalloc.h>
#include <unistd.h>
#include <breakwater/breakwater.h>
#include <breakwater/seda.h>
#include <breakwater/dagor.h>
#include <breakwater/nocontrol.h>
}

#include "cc/net.h"
#include "cc/runtime.h"
#include "cc/sync.h"
#include "cc/thread.h"
#include "cc/timer.h"
#include "breakwater/rpc++.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <atomic>

#include <ctime>

using namespace std::chrono;
using sec = duration<double, std::micro>;

const struct crpc_ops *crpc_ops;
const struct srpc_ops *srpc_ops;

using Arrival = std::function<double()>;
using TermGen = std::function<uint64_t()>;

constexpr uint64_t kMaxCatchUpUS = 5;
constexpr uint64_t kRTT = 10;
constexpr uint64_t kWarmUpTime = 4000000;
constexpr uint64_t kExperimentTime = 8000000;

struct payload
{
  uint64_t term_index;
  uint64_t index;
  uint64_t hash;
};

struct work_unit
{
  double start_us;
  double latency_us;
  uint64_t tsc_end;
  bool sent; 
  bool received;

  // uint64_t term_index; 
  // uint64_t index; 
  // uint64_t hash; 
}; 

int
StringToAddr(const char *str, uint32_t *addr)
{
  uint8_t a, b, c, d;

  if (sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4)
    return -EINVAL;

  *addr = MAKE_IP_ADDR(a, b, c, d);
  return 0;
}

netaddr raddr;

int threads;
uint64_t num_terms;
double target_rps;
uint64_t completed_reqs = 0;



// work factory generates a batch of work units for the client to execute.
// uses poisson distribution to calculate dispatch times for each request in the batch


std::vector<work_unit> GenerateWork(Arrival a, double cur_us, double last_us) {
  uint64_t req_id = 0; 
  std::vector<work_unit> w;
  while (true) {
    cur_us += a();
    if (cur_us > last_us) break;
    w.emplace_back(work_unit{
      cur_us,                     // start_us 
      0,                          // latency_us             
      0,                          // tsc_end 
      false,                      // sent
      false,                      // received
    });
  }
  return w;
}

std::vector<work_unit> OpenLoopClientWorker(
    int id,
    // rpc::RpcClient *client,
    rt::WaitGroup *starter,
    rt::WaitGroup *starter2,
    std::atomic<uint64_t> &global_completed_reqs, 
    std::function<std::vector<work_unit>()> work_factory
) {

  std::cout << "Inside ClientWorker " << id << "\n"
            << std::flush;

  struct rpc_session_info info = {.session_type = 0};
  std::unique_ptr<rpc::RpcClient> c(rpc::RpcClient::Dial(raddr, id + 1, nullptr, nullptr, &info));
  if (!c)
  {
    std::cerr << "Thread " << id << ": Failed to dial server" << std::endl;
    return {}; 
  }

  std::vector<work_unit> work = work_factory();

  std::vector<uint64_t> timings;
  // timings.reserve(work.size());
  timings.resize(work.size()); 

  auto receiver_th = rt::Thread([&]{

    char resp_buf[4096];
    while (true) {
      ssize_t ret = c->Recv(resp_buf, sizeof(resp_buf), 0, nullptr);
      if (ret <= 0) break; 

      uint64_t now = microtime();
            
      payload *msg = reinterpret_cast<payload *>(resp_buf);
      uint64_t idx = ntoh64(msg->index); 

      if (idx < work.size()) {
        work[idx].latency_us = now - timings[idx]; 
        work[idx].received = true;
        global_completed_reqs.fetch_add(1, std::memory_order_relaxed);
      }
  } }); 

  std::mt19937 rng(id);
  std::uniform_int_distribution<uint64_t> term_dist(0, num_terms - 1);

  starter->Done();
  starter2->Wait();

  barrier();
  auto expstart = steady_clock::now();
  barrier();

  char buf[4096];
  payload p;

  for (unsigned int i = 0; i < work.size(); ++i) {
    barrier();
    auto now = steady_clock::now();
    barrier();
    if (duration_cast<sec>(now - expstart).count() < work[i].start_us) {
      rt::Sleep(work[i].start_us - duration_cast<sec>(now - expstart).count());
    }
    if (duration_cast<sec>(now - expstart).count() - work[i].start_us > kMaxCatchUpUS)
      continue;

    timings[i] = microtime();
  
    p.term_index = hton64(term_dist(rng));
    p.index = hton64(i);
    p.hash = hton64(rand());

    ssize_t ret = c->Send(&p, sizeof(p), p.index, nullptr);

    // Send an RPC request.
    if (ret == sizeof(p)) 
      work[i].sent = true; 
    if (ret == -ENOBUFS) continue;
    if (ret != static_cast<ssize_t>(sizeof(p)))
      panic("write failed, ret = %ld", ret);
  }

  // rt::Sleep(1 * rt::kSeconds);
  rt::Sleep((int)(kRTT + 2));
  BUG_ON(c->Shutdown(SHUT_RDWR));
  receiver_th.Join();

  return work; 
}


void PoissonExperimentHandler(void *arg) {
  rt::WaitGroup starter(threads); 
  rt::WaitGroup starter2(1); 
  std::atomic<uint64_t> global_success_count{0};

  std::vector<rt::Thread> th;
  std::unique_ptr<std::vector<work_unit>> samples[threads];
  for (int i = 0; i < threads; ++i) {
    th.emplace_back(rt::Thread([&, i] {
      std::random_device rd_device;
      std::mt19937 rg(rd_device() + i); 
  
      double thread_rps = target_rps / static_cast<double>(threads);
      std::exponential_distribution<double> rd(1.0 / (1000000.0 / thread_rps));
      std::vector<work_unit> v = OpenLoopClientWorker(i, &starter, &starter2, global_success_count, [&rg, &rd] {
        return GenerateWork(std::bind(rd, rg), 0, kExperimentTime);
      });
      
      samples[i].reset(new std::vector<work_unit>(std::move(v)));
      // samples[i] = std::make_unique<std::vector<work_unit>>(std::move(v));
    })); 
  } 

  starter.Wait(); 
  starter2.Done(); 

  auto start = steady_clock::now();
  for (auto &t : th) t.Join();
  auto finish = steady_clock::now();

  double elapsed_ = duration_cast<sec>(finish - start).count();

  // after this, handle extracting all the information from samples and printing results   
}


void SimpleClientWorker(int id)
{

  std::cout << "Inside ClientWorker " << id << "\n"
            << std::flush;

  struct rpc_session_info info = {.session_type = 0};
  // auto c = rpc::RpcClient::Dial(raddr, id + 1, nullptr, nullptr, nullptr);
  std::unique_ptr<rpc::RpcClient> c(rpc::RpcClient::Dial(raddr, id + 1, nullptr, nullptr, &info));

  if (!c)
  {
    std::cerr << "Thread " << id << ": Failed to dial server" << std::endl;
    return;
  }

  std::mt19937 rng(id);
  std::uniform_int_distribution<uint64_t> term_dist(0, num_terms - 1);

  // Calculate pacing
  double per_thread_rps = target_rps / threads;
  uint64_t interval_us = static_cast<uint64_t>(1000000.0 / per_thread_rps);

  char resp[4096];
  uint64_t req_id = 0;

  std::cout << "Entering request loop\n"
            << std::flush;
  while (true)
  {
    payload p;
    p.term_index = hton64(term_dist(rng));
    p.index = req_id++;
    p.hash = rand();

    std::cout << "Sending request" << std::flush;
    ssize_t ret = c->Send(&p, sizeof(p), p.index, nullptr);
    if (ret != sizeof(p))
    {
      rt::Yield(); // Yield if send fails to prevent tight spinning
      continue;
    }

    // receive response (blocking)
    ret = c->Recv(resp, sizeof(resp), 0, nullptr);
    std::cout << "Received response" << std::flush;
    if (ret <= 0)
    {
      continue;
    }
    else
    {
      completed_reqs++;
    }

    if (completed_reqs % 10000 == 0)
    {
      std::cout << "Thread " << id << " finished " << completed_reqs << " requests." << std::flush;
    }

    std::cout << "Sleeping" << std::flush;
    rt::Sleep(interval_us);
  }
}

void MainHandler(void *arg)
{
  std::cout << "In main handler\n"
            << std::flush;
  std::vector<rt::Thread> workers;

  for (int i = 0; i < threads; i++)
  {
    // Caladan threads are spawned using rt::Thread([lambda])
    std::cerr << "Spawning worker thread " << i << "\n"
              << std::endl;
    workers.emplace_back(rt::Thread([=]
                                    { SimpleClientWorker(i); }));
  }

  for (auto &t : workers)
  {
    t.Join();
  }
  std::cerr << "Finished\n"
            << std::endl;
}

void TestMainHandler(void *arg)
{
  std::cout << "In test main handler\n"
            << std::flush;
  printf("In test main handler\n");
}

int main(int argc, char *argv[])
{
  if (argc < 7)
  {
    std::cerr << "usage:\n"
              << "[alg] [cfg] client [threads] [server_ip] [num_terms] [rps]\n";
    return -EINVAL;
  }

  std::string olc = argv[1];
  if (olc.compare("breakwater") == 0)
  {
    crpc_ops = &cbw_ops;
    srpc_ops = &sbw_ops;
  }
  else if (olc.compare("breakwater2") == 0)
  {
    crpc_ops = &cbw_ops;
    srpc_ops = &sbw2_ops;
  }
  else if (olc.compare("seda") == 0)
  {
    crpc_ops = &csd_ops;
    srpc_ops = &ssd_ops;
  }
  else if (olc.compare("dagor") == 0)
  {
    crpc_ops = &cdg_ops;
    srpc_ops = &sdg_ops;
  }
  else if (olc.compare("nocontrol") == 0)
  {
    crpc_ops = &cnc_ops;
    srpc_ops = &snc_ops;
  }
  else
  {
    std::cerr << "invalid algorithm: " << olc << std::endl;
    std::cerr << "usage: [alg] [cfg_file]\n"
              << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
              << "\tcfg_file: Shenango configuration file\n"
              << std::endl;
    return -EINVAL;
  }

  try
  {
    threads = std::stoi(argv[4]);
    num_terms = std::stoull(argv[6]);
    target_rps = std::stod(argv[7]);
  }
  catch (...)
  {
    std::cerr << "Invalid arguments\n";
    return -EINVAL;
  }

  std::cout << "Parsed arguments\n"
            << std::endl;

  if (StringToAddr(argv[5], &raddr.ip))
  {
    std::cout << "Invalid server IP\n"
              << std::endl;
    std::cerr << "Invalid server IP\n";
    return -EINVAL;
  }
  std::cout << "Valid server IP\t" << std::flush;
  raddr.port = 8001;

  // printf("%s:%d\n", raddr.ip, raddr.port);

  // ip_addr_to_str
  std::cout << argv[5] << ":" << raddr.port << std::endl;

  // Initialize the Caladan runtime
  // int ret = runtime_init(argv[2], MainHandler, NULL);
  int ret = runtime_init(argv[2], PoissonExperimentHandler, NULL);

  if (ret)
  {
    std::cerr << "runtime init failed\n";
    std::cout << "Caladan runtime init failed\n"
              << std::endl;
    return ret;
  }

  return 0;
}