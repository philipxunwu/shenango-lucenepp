/////////////////////////////////////////////////////////////////////////////
// Copyright (c) 2009-2014 Alan Wright. All rights reserved.
// Distributable under the terms of either the Apache License (Version 2.0)
// or the GNU Lesser General Public License.
/////////////////////////////////////////////////////////////////////////////

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#ifndef NOMINMAX
#define NOMINMAX
#endif

extern "C" {
#include <base/log.h>
#include <breakwater/breakwater.h>
#include <breakwater/seda.h>
#include <breakwater/dagor.h>
#include <breakwater/nocontrol.h>
}

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <string>
#include "LuceneHeaders.h"
#include "Document.h"
#include "Field.h"
#include "Term.h"
#include "TermQuery.h"
#include "TopFieldDocs.h"
#include "IndexWriter.h"
#include "IndexSearcher.h"
#include "KeywordAnalyzer.h"
#include "Query.h"
#include "cc/net.h"
#include "cc/runtime.h"
#include "breakwater/rpc++.h"

const struct crpc_ops *crpc_ops;
const struct srpc_ops *srpc_ops;

using namespace Lucene;

constexpr uint64_t numDocs = 1000000;
constexpr uint64_t numWarmUpSamples = 10;
constexpr uint64_t numSamples = 1;
constexpr uint64_t NORM = 100;
constexpr int searchN = 100;

atomic64_t acc_hits;
atomic64_t num_resp;

#define NDIR 1

std::vector<String> terms;
std::vector<uint64_t> frequencies;
uint64_t weight_sum;
RAMDirectoryPtr dir;

static IndexReaderPtr readers[NCPU];
static IndexSearcherPtr searchers[NCPU];

struct payload {
  uint64_t term_index;
  uint64_t index;
  uint64_t hash;
};

constexpr uint64_t kRPCSStatPort = 8002;
constexpr uint64_t kRPCSStatMagic = 0xDEADBEEF;
struct sstat_raw {
  uint64_t idle;
  uint64_t busy;
  unsigned int num_cores;
  unsigned int max_cores;
  uint64_t winu_rx;
  uint64_t winu_tx;
  uint64_t win_tx;
  uint64_t req_rx;
  uint64_t req_dropped;
  uint64_t resp_tx;
};

void ReadFreqTerms() {
  std::cout << "Reading csv ...\t" << std::flush;
  std::ifstream fin("frequent_terms.csv");

  std::string line, word;
  String wword;
  uint64_t freq;

  weight_sum = 0;

  while(std::getline(fin, line)) {
    std::stringstream ss(line);

    getline(ss, word, ',');
    wword = String(word.length(), L' ');
    std::copy(word.begin(), word.end(), wword.begin());
    terms.push_back(wword);

    getline(ss, word, ',');
    freq = std::stoi(word) / NORM;
    frequencies.push_back(freq);
    weight_sum += freq;
  }

  fin.close();
  std::cout << "Done" << std::endl;
}

String ChooseTerm() {
  uint64_t rand_ = (uint64_t)rand() % weight_sum;
  int i;

  for(i = 0; i < frequencies.size(); ++i) {
    if (rand_ < frequencies[i]) {
      break;
    } else {
      rand_ -= frequencies[i];
    }
  }

  return terms[i];
}

String ChooseTerm(uint64_t hash) {
  uint64_t rand_ = hash % weight_sum;
  int i;

  for(i = 0; i < frequencies.size(); ++i) {
    if (rand_ < frequencies[i]) {
      break;
    } else {
      rand_ -= frequencies[i];
    }
  }

  return terms[i];
}

DocumentPtr createDocument(const String& contents) {
  DocumentPtr document = newLucene<Document>();
  document->add(newLucene<Field>(L"contents", contents, Field::STORE_YES, Field::INDEX_ANALYZED));
  return document;
}

void PopulateIndex() {
  std::cout << "Populating indices ...\t" << std::flush;
  uint64_t start = microtime();
  int num_docs = 0;
  IndexWriterPtr indexWriter;

  dir = newLucene<RAMDirectory>();
  indexWriter = newLucene<IndexWriter>(dir, newLucene<StandardAnalyzer>(LuceneVersion::LUCENE_CURRENT), true,
				       IndexWriter::MaxFieldLengthLIMITED);

  std::string line;
  String wline;
  // std::ifstream tweet_txt("2021-11-27-dataset-text.tsv");
  std::ifstream tweet_txt("synthetic_dataset_for_lucene.tsv");

  if (!tweet_txt.is_open()) {
    std::cout << "Unable to open file" << std::endl;
    return;
  }

  while (getline(tweet_txt, line)) {
    wline = String(line.length(), L' ');
    std::copy(line.begin(), line.end(), wline.begin());
    indexWriter->addDocument(createDocument(wline));
    num_docs++;
  }
  tweet_txt.close();

  indexWriter->optimize();
  indexWriter->close();

  uint64_t finish = microtime();
  std::cout << "Done: " << num_docs << " documents (" << (finish - start) / 1000000.0 << " s)" << std::endl;
}

void RPCSStatWorker(std::unique_ptr<rt::TcpConn> c) {
  while (true) {
    // Receive an uptime request.
    uint64_t magic;
    ssize_t ret = c->ReadFull(&magic, sizeof(magic));
    if (ret != static_cast<ssize_t>(sizeof(magic))) {
      if (ret == 0 || ret == -ECONNRESET) break;
      log_err("read failed, ret = %ld", ret);
      break;
    }

    // Check for the right magic value.
    if (ntoh64(magic) != kRPCSStatMagic) break;

    // Calculate the current uptime.
    std::ifstream file("/proc/stat");
    std::string line;
    std::getline(file, line);
    std::istringstream ss(line);
    std::string tmp;
    uint64_t user, nice, system, idle, iowait, irq, softirq, steal, guest,
        guest_nice;
    ss >> tmp >> user >> nice >> system >> idle >> iowait >> irq >> softirq >>
        steal >> guest >> guest_nice;
    sstat_raw u = {idle + iowait,
                   user + nice + system + irq + softirq + steal,
                   rt::RuntimeMaxCores(),
                   static_cast<unsigned int>(sysconf(_SC_NPROCESSORS_ONLN)),
                   rpc::RpcServerStatCupdateRx(),
                   rpc::RpcServerStatEcreditTx(),
                   rpc::RpcServerStatCreditTx(),
                   rpc::RpcServerStatReqRx(),
                   rpc::RpcServerStatReqDropped(),
                   rpc::RpcServerStatRespTx()};

    // Send an uptime response.
    ssize_t sret = c->WriteFull(&u, sizeof(u));
    if (sret != sizeof(u)) {
      if (sret == -EPIPE || sret == -ECONNRESET) break;
      log_err("write failed, ret = %ld", sret);
      break;
    }
  }
}

void RPCSStatServer() {
  std::unique_ptr<rt::TcpQueue> q(
      rt::TcpQueue::Listen({0, kRPCSStatPort}, 4096));
  if (q == nullptr) panic("couldn't listen for connections");

  while (true) {
    rt::TcpConn *c = q->Accept();
    if (c == nullptr) panic("couldn't accept a connection");
    rt::Thread([=] { RPCSStatWorker(std::unique_ptr<rt::TcpConn>(c)); })
        .Detach();
  }
}

void RequestHandler(struct srpc_ctx *ctx) {
  std::cout << "Received request\n" << std::flush;
  if (unlikely(ctx->req_len != sizeof(payload))) {
    log_err("got invalid RPC len %ld", ctx->req_len);
    return;
  }
  const payload *in = reinterpret_cast<const payload *>(ctx->req_buf);
  int core_id = get_current_affinity();

  
  QueryPtr query = newLucene<TermQuery>(newLucene<Term>(L"contents", terms[ntoh64(in->term_index)]));
  TopDocsPtr result = searchers[core_id]->search(query, FilterPtr(), searchN);
  Collection<ScoreDocPtr> hits = result->scoreDocs;

  ctx->resp_len = sizeof(payload);
  ctx->drop = false; 
  payload *out = reinterpret_cast<payload *>(ctx->resp_buf);
  memcpy(out, in, sizeof(*out));
}

void MainHandler(void *arg) {
  uint64_t start = microtime();
  rt::Thread([] { RPCSStatServer(); }).Detach();

  atomic64_write(&acc_hits, 0);
  atomic64_write(&num_resp, 0);
  srand(time(NULL));

  ReadFreqTerms();

  PopulateIndex();

  for (int i = 0; i < NCPU; ++i) {
    /*
    Collection<IndexReaderPtr> readers = Collection<IndexReaderPtr>::newInstance(NDIR);
    for (int j = 0; j < NDIR; ++j) {
      readers[j] = IndexReader::open(dir[j], true);
    }

    MultiReaderPtr mreader = newLucene<MultiReader>(readers);
    searchers[i] = newLucene<IndexSearcher>(mreader);
    */
    readers[i] = IndexReader::open(dir, true);
    searchers[i] = newLucene<IndexSearcher>(readers[i]);
  }

  uint64_t finish = microtime();
  printf("Ready to run the server... %lf s\n", (finish - start) / 1000000.0);
  int ret = rpc::RpcServerEnable(RequestHandler);
  if (ret) panic("couldn't enable RPC server");

  rt::WaitGroup(1).Wait();
}

int main(int argc, char* argv[]) {
  int ret;

  std::string olc = argv[1]; // overload control
  if (olc.compare("breakwater") == 0) {
    crpc_ops = &cbw_ops;
    srpc_ops = &sbw_ops;
  } else if (olc.compare("breakwater2") == 0) {
    crpc_ops = &cbw_ops;
    srpc_ops = &sbw2_ops;
  } else if (olc.compare("seda") == 0) {
    crpc_ops = &csd_ops;
    srpc_ops = &ssd_ops;
  } else if (olc.compare("dagor") == 0) {
    crpc_ops = &cdg_ops;
    srpc_ops = &sdg_ops;
  } else if (olc.compare("nocontrol") == 0) {
    crpc_ops = &cnc_ops;
    srpc_ops = &snc_ops;
  } else {
    std::cerr << "invalid algorithm: " << olc << std::endl;
    std::cerr << "usage: [alg] [cfg_file]\n"
	      << "\talg: overload control algorithms (breakwater/seda/dagor)\n"
	      << "\tcfg_file: Shenango configuration file\n" << std::endl;
    return -EINVAL;
  }

  ret = runtime_init(argv[2], MainHandler, NULL);
  if (ret) {
    printf("failed to start runtime\n");
    return ret;
  }

  return 0;
}
