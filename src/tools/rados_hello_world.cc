#include <iostream>
#include "include/rados/librados.hpp"

int main() {
  std::string ceph_conf = "./ceph.conf";

  rados_t cluster;
  int32_t error_code = rados_create(&cluster, NULL);
  if (error_code < 0) {
    std::cerr << "failed to rados_create: error_code " << error_code << std::endl;
    return -1;
  }

  error_code = rados_conf_read_file(cluster, ceph_conf.c_str());
  if (error_code < 0) {
    std::cerr << "failed to rados_conf_read_file: ceph_conf " << ceph_conf
        << " error_code " << error_code << std::endl;
    return -1; 
  }

  error_code = rados_connect(cluster);
  if (error_code < 0) {
    std::cerr << "failed to rados_connect: error_code " << error_code << std::endl;
    return -1; 
  } 


  rados_ioctx_t io;
  error_code = rados_ioctx_create(cluster, "testpool", &io);
  if (error_code < 0) {
    std::cerr << "failed to rados_ioctx_create: error_code " << error_code << std::endl;
    rados_shutdown(cluster);
    return -1;
  }

  error_code = rados_write_full(io, "greeting", "hello", 5);
  if (error_code < 0) {
    std::cerr << "failed to rados_write_full: error_code " << error_code << std::endl;
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    return -1;
  }

  uint64_t last_version = rados_get_last_version(io);
  std::cout << "last_version " << last_version << std::endl;

  rados_ioctx_destroy(io);
  rados_shutdown(cluster);

  return 0;
}
