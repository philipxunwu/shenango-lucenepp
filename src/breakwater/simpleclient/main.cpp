extern "C" {
#include <base/log.h>
#include <runtime/runtime.h>
}

#include "cc/net.h"
#include "cc/runtime.h"
#include "breakwater/rpc++.h"

#include <iostream>
#include <random>
#include <chrono>

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

using namespace std::chrono;

void ClientWorker(int id) {
  rpc::RpcClient c(raddr, id + 1);

  std::mt19937 rng(id);
  std::uniform_int_distribution<uint64_t> term_dist(0, num_terms - 1);

  double per_thread_rps = target_rps / threads;
  double interval_us = 1000000.0 / per_thread_rps;

  char resp[4096];

  uint64_t req_id = 0;

  auto next_send = steady_clock::now();

  while (true) {
    payload p;
    p.term_index = term_dist(rng);
    p.index = req_id++;
    p.hash = rand();

    ssize_t ret = c.Send(&p, sizeof(p), p.index, nullptr);
    if (ret != sizeof(p)) {
      continue;
    }

    // receive response (blocking)
    ret = c.Recv(resp, sizeof(resp), 0, nullptr);
    if (ret <= 0) {
      continue;
    }

    // pace requests
    next_send += microseconds((int)interval_us);
    std::this_thread::sleep_until(next_send);
  }
}

void MainHandler(void *arg) {
  std::vector<rt::Thread> workers;

  for (int i = 0; i < threads; i++) {
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

  threads = std::stoi(argv[4]);

  if (StringToAddr(argv[5], &raddr.ip)) return -EINVAL;
  raddr.port = 8001;

  num_terms = std::stoull(argv[6]);
  target_rps = std::stod(argv[7]);

  int ret = runtime_init(argv[2], MainHandler, NULL);
  if (ret) {
    std::cerr << "runtime init failed\n";
    return ret;
  }

  return 0;
}