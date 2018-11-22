// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_BENCH_RGW_BENCH_H
#define CEPH_RGW_BENCH_RGW_BENCH_H

#include <string>
#include "common/WorkQueue.h"

class RGWBench {
 public:
  struct Config {
    std::string rgw_address = "127.0.0.1:8000";
    std::string access_key = "0555b35654ad1656d804";
    std::string secret_key = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";
    int thread_number = 1;
    int object_size = 1024 * 1024;
    int object_count = 1024;
    int bench_secs = 10;
  };

  RGWBench(CephContext* cct, const Config& config);
  ~RGWBench();

  bool prepare();
  void execute();
  bool cleanup();

 private:
  void worker();

  CephContext* cct_;
  Config config_;
};

#endif  // CEPH_RGW_BENCH_RGW_BENCH_H
