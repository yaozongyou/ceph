// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>
#include <iostream>
#include "rgw_s3_client.h"

static size_t CurlReadCallbackWrapper(char *buffer, size_t size, size_t nitems, void *instream) {
  if (instream == nullptr) {
    return 0;
  }
  std::string_view* content = static_cast<std::string_view*>(instream);
  std::size_t length = std::min(size * nitems, content->size());
  memcpy(buffer, content->data(), length);
  content->remove_prefix(length);
  return length;
}

static size_t CurlWriteCallbackWrapper(char *ptr, size_t size, size_t nmemb, void *userdata) {
  if (userdata != nullptr) {
    (static_cast<std::string*>(userdata))->append(ptr, size * nmemb);
  }
  return size * nmemb;
}

int main(int argc, const char* argv[]) {
  std::string rgw_address = "127.0.0.1:8000";
  std::string access_key = "0555b35654ad1656d804";
  std::string secret_key = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";
  RGWS3Client s3_client(rgw_address, access_key, secret_key);

  s3_client.create_bucket("test_bucket");
  std::string_view content = "Hello World!";
  s3_client.put_object("test_bucket", "hello.txt", content.size(), CurlReadCallbackWrapper, &content);
  
  std::string aaa;
  s3_client.get_object("test_bucket", "hello.txt", CurlWriteCallbackWrapper, &aaa);
  std::cout << "content " << aaa << std::endl;

  s3_client.delete_object("test_bucket", "hello.txt");
  s3_client.remove_bucket("test_bucket");

  return 0;
}
