// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_bench.h"
#include <getopt.h>
#include <cstring>
#include <iostream>
#include <thread> 
#include "rgw_s3_client.h"
#include "common/log_message.h"

RGWBench::RGWBench(const Config& config) : config_(config) {
}

RGWBench::~RGWBench() {
}

static size_t curl_read_callback_wrapper(char *buffer, size_t size, size_t nitems, void *instream) {
  std::size_t* content_length = static_cast<std::size_t*>(instream);
  std::size_t length = std::min(size * nitems, *content_length);
  *content_length -= length;
  return length;
}

static size_t curl_write_callback_wrapper(char *ptr, size_t size, size_t nmemb, void *userdata) {
  if (userdata != nullptr) {
    (static_cast<std::string*>(userdata))->append(ptr, size * nmemb);
  }
  return size * nmemb;
}

bool RGWBench::prepare() {
  std::cout << "prepare bench environment" << std::endl;
  RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);
  if (!s3_client.create_bucket("radosgw-bench-bucket")) {
    std::cout << "failed to create bucket" << std::endl;
    return false;
  }

  if (config_.bench_type == "read") {
    RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);

    for (int i = 0; i < config_.object_count; ++i) {
      std::string key = std::to_string(i);
      std::size_t content_length = config_.object_size;
      s3_client.put_object("radosgw-bench-bucket", key, content_length, curl_read_callback_wrapper, &content_length);
    }
  }

  std::cout << "prepare success" << std::endl;
  return true;
}

void RGWBench::worker() {
  RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);
  std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<> dis(0, config_.object_count-1);

  time_t start_timestamp = time(NULL);
  while ((time(NULL) - start_timestamp) <= config_.bench_secs) {
    std::string key = std::to_string(dis(generator));
    
    if (config_.bench_type == "write") {
      std::size_t content_length = config_.object_size;
      s3_client.put_object("radosgw-bench-bucket", key, content_length, curl_read_callback_wrapper, &content_length);
    } else {
      s3_client.get_object("radosgw-bench-bucket", key, curl_write_callback_wrapper, nullptr);
    }
  }
}

void RGWBench::execute() {
  std::vector<std::thread> threads;
  for (int i = 0; i < config_.thread_number; i++) {
    threads.push_back(std::thread(&RGWBench::worker, this));
  }

  for (auto& thread : threads) {
    thread.join();
  }
}

bool RGWBench::cleanup() {
  std::cout << "start to cleanup" << std::endl;
  LOG(INFO) << "start to cleanup";
  std::vector<std::thread> threads;
  for (int i = 0; i < config_.thread_number; i++) {
    threads.push_back(std::thread([this, i]{
           RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);
           for (int j = 0; j < config_.object_count; ++j) {
	     if ((j % config_.thread_number) == i) {
               s3_client.delete_object("radosgw-bench-bucket", std::to_string(j));
	     }
           }
        }));
  }

  for (auto& thread : threads) {
    thread.join();
  }

  RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);
  s3_client.remove_bucket("radosgw-bench-bucket");
  
  std::cout << "cleanup finished" << std::endl;
  LOG(INFO) << "cleanup finished";

  return true;
}
