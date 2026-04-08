extern "C" {
#include <base/log.h>
#include <runtime/runtime.h>
#include <net/ip.h>
}

#include "cc/net.h"
#include "cc/runtime.h"
#include "cc/thread.h"
#include "breakwater/rpc++.h"

#include <iostream>
#include <random>
#include <chrono>
#include <vector>
#include <memory>

const struct crpc_ops *crpc_ops;

struct payload {
  uint64_t term_index;
  uint64_t index;
  uint64_t hash;
};

netaddr raddr;

int threads;
uint64_t num_terms;
double target_rps;
uint64_t completed_reqs = 0;

void ClientWorker(int id) {
  auto c = rpc::RpcClient::Dial(raddr, id + 1, nullptr, nullptr, nullptr);
  
  if (!c) {
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

  while (true) {
    payload p;
    p.term_index = term_dist(rng);
    p.index = req_id++;
    p.hash = rand();


    ssize_t ret = c->Send(&p, sizeof(p), p.index, nullptr);
    if (ret != sizeof(p)) {
      rt::Yield(); // Yield if send fails to prevent tight spinning
      continue;
    }

    // receive response (blocking)
    ret = c->Recv(resp, sizeof(resp), 0, nullptr);
    if (ret <= 0) {
      continue;
    } else {
      completed_reqs++; 
    }

    if (completed_reqs % 10000 == 0) {
        std::cout << "Thread " << id << " finished " << completed_reqs << " requests." << std::endl;
    }

    rt::Sleep(interval_us);
  }
}

void MainHandler(void *arg) {
  std::vector<rt::Thread> workers;

  for (int i = 0; i < threads; i++) {
    // Caladan threads are spawned using rt::Thread([lambda])
    workers.emplace_back(rt::Thread([=] { ClientWorker(i); }));
  }

  for (auto &t : workers) {
    t.Join();
  }
}

int main(int argc, char *argv[]) {
  if (argc < 7) {
    std::cerr << "usage:\n"
              << "[alg] [cfg] client [threads] [server_ip] [num_terms] [rps]\n";
    return -EINVAL;
  }

  std::string alg = argv[1];
  if (alg == "breakwater") {
    crpc_ops = &cbw_ops;
  } else if (alg == "seda") {
    crpc_ops = &csd_ops;
  } else if (alg == "dagor") {
    crpc_ops = &cdg_ops;
  } else {
    crpc_ops = &cnc_ops;
  }

  try {
    threads = std::stoi(argv[4]);
    num_terms = std::stoull(argv[6]);
    target_rps = std::stod(argv[7]);
  } catch (...) {
    std::cerr << "Invalid arguments\n";
    return -EINVAL;
  }

  // StringToAddr is available because of #include <net/ip.h>
  if (StringToAddr(argv[5], &raddr.ip)) {
    std::cerr << "Invalid server IP\n";
    return -EINVAL;
  }
  raddr.port = 8001;

  // Initialize the Caladan runtime
  int ret = runtime_init(argv[2], MainHandler, NULL);
  if (ret) {
    std::cerr << "runtime init failed\n";
    return ret;
  }

  return 0;
}