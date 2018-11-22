// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_bench.h"
#include <getopt.h>
#include <cstring>
#include <iostream>
#include <thread> 
#include "rgw_s3_client.h"
#include "include/uuid.h"

RGWBench::RGWBench(CephContext* cct, const Config& config) : cct_(cct), config_(config) {
}

RGWBench::~RGWBench() {
}

static size_t curl_read_callback_wrapper(char *buffer, size_t size, size_t nitems, void *instream) {
  std::size_t* content_length = static_cast<std::size_t*>(instream);
  std::size_t length = std::min(size * nitems, *content_length);
  *content_length -= length;
  return length;
}

/*
static size_t CurlWriteCallbackWrapper(char *ptr, size_t size, size_t nmemb, void *userdata) {
  if (userdata != nullptr) {
    (static_cast<std::string*>(userdata))->append(ptr, size * nmemb);
  }
  return size * nmemb;
}
*/


bool RGWBench::prepare() {
  RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);
  return s3_client.create_bucket("radosgw-bench-bucket");
}

void RGWBench::worker() {
  RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);
  std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<> dis(0, config_.object_count-1);

  time_t start_timestamp = time(NULL);
  while ((time(NULL) - start_timestamp) < config_.bench_secs) {
    std::string key = std::to_string(dis(generator));
    std::size_t content_length = config_.object_size;
    s3_client.put_object("radosgw-bench-bucket", key, content_length, curl_read_callback_wrapper, &content_length);
  }
}

void RGWBench::execute() {
  std::vector<std::thread> workers;

  for (int i = 0; i < config_.thread_number; i++) {
    workers.push_back(std::thread(&RGWBench::worker, this));
  }

  for (int i = 0; i < config_.thread_number; ++i) {
    workers[i].join();
  }
}

bool RGWBench::cleanup() {
  class WQ : public ThreadPool::WorkQueueVal<std::string> {
   public:
    WQ(std::string n, time_t ti, time_t sti, ThreadPool *p, Config *config) : 
        ThreadPool::WorkQueueVal<std::string>(std::move(n), ti, sti, p), config_(config) {
    }

    void _enqueue(std::string s) override {
      queue_.push_back(s);
    }

    void _enqueue_front(std::string s) override {
      queue_.push_front(s);
    }

    bool _empty() override {
      return queue_.empty();
    }

    std::string _dequeue() override {
      std::string s = queue_.front();
      queue_.pop_front();
      return s;
    }
    
    void _process(std::string key, ThreadPool::TPHandle &) override {
      std::cout << "key " << key << std::endl;
      RGWS3Client s3_client(config_->rgw_address, config_->access_key, config_->secret_key);
      s3_client.delete_object("radosgw-bench-bucket", key);
    }

   private:
    std::list<std::string> queue_;
    Config* config_;
  };
 
  ThreadPool p(cct_, "", "", 10, nullptr);
  WQ wq("", 10, 10, &p, &config_);
  p.start();

  std::list<std::string> prefixes;
  prefixes.push_front("");

  RGWS3Client s3_client(config_.rgw_address, config_.access_key, config_.secret_key);

  while (!prefixes.empty()) {
    std::string prefix = prefixes.front();
    prefixes.pop_front();

    for (;;) {
      bool is_truncated = false;
      std::string next_marker;
      std::vector<std::string> keys;
      std::vector<std::string> dirs;

      s3_client.list_objects(
	  "radosgw-bench-bucket",
	  "/",  // delimiter
	  next_marker,
	  1000, // max_keys
	  prefix,
	  &is_truncated, &next_marker, &keys, &dirs);

      for (std::vector<std::string>::const_iterator iter = dirs.begin(); iter != dirs.end(); ++iter) {
	prefixes.push_front(*iter);
      }

      for (std::vector<std::string>::const_iterator iter = keys.begin();
	  iter != keys.end(); ++iter) {
	wq.queue(*iter);
      }

      if (!is_truncated) {
	break;
      }
    }
  }

  wq.drain();
  p.stop();

  s3_client.remove_bucket("radosgw-bench-bucket");

  return true;
}
