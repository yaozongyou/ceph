// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_BENCH_RGW_S3_CLIENT_H
#define CEPH_RGW_BENCH_RGW_S3_CLIENT_H

#include <functional>
#include <string>
#include <vector>
#include <curl/curl.h>

class RGWS3Client {
 public:
  RGWS3Client(const std::string& rgw_address, const std::string& access_key, const std::string& secret_key);
  ~RGWS3Client();
  
  typedef size_t read_callback_t(char *ptr, size_t size, size_t nmemb, void *user_data);
  typedef size_t write_callback_t(char *ptr, size_t size, size_t nmemb, void *user_data);

  bool create_bucket(const std::string& bucket);
  bool remove_bucket(const std::string& bucket);

  bool put_object(const std::string& bucket, const std::string& key, uint64_t content_length, 
      read_callback_t* callback, void* user_data);
  bool get_object(const std::string& bucket, const std::string& key, 
      write_callback_t* callback, void* user_data);
  bool delete_object(const std::string& bucket, const std::string& key);

  bool list_objects(
      const std::string& bucket,
      const std::string& delimiter,
      const std::string& marker,
      int max_keys,
      const std::string& prefix,
      bool* is_truncated,
      std::string* next_marker,
      std::vector<std::string>* objects,
      std::vector<std::string>* dirs);

private:
  void reset();
  std::string get_date();
  std::string create_signature(const std::string& string_to_sign);

  std::string rgw_address_;
  std::string access_key_;
  std::string secret_key_;

  CURL* curl_;
};

#endif  // CEPH_RGW_BENCH_RGW_S3_CLIENT_H
