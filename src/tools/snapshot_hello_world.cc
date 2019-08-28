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

  /*
  rados_snap_t snapid1 = 0;
  error_code = rados_ioctx_selfmanaged_snap_create(io, &snapid1);
  if (error_code < 0) {
    std::cerr << "failed to rados_ioctx_selfmanaged_snap_create: error_code " << error_code << std::endl;
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    return -1;
  }
  std::cout << "snapid1 " << snapid1 << std::endl;

  rados_snap_t snaps[2] = {snapid1};
  rados_ioctx_selfmanaged_snap_set_write_ctx(io, snapid1, snaps, 1);
  */
  error_code = rados_write_full(io, "hello", "world", 5);
  if (error_code < 0) {
    std::cerr << "failed to rados_write_full: error_code " << error_code << std::endl;
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    return -1;
  }

  rados_snap_t snapid2 = 0;
  error_code = rados_ioctx_selfmanaged_snap_create(io, &snapid2);
  if (error_code < 0) {
    std::cerr << "failed to rados_ioctx_selfmanaged_snap_create: error_code " << error_code << std::endl;
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    return -1;
  }

  std::cout << "snapid2 " << snapid2 << std::endl;
  
  rados_snap_t snaps[2];
  snaps[0] = snapid2;
  //snaps[1] = snapid1;
  rados_ioctx_selfmanaged_snap_set_write_ctx(io, snapid2, snaps, 1);
  error_code = rados_write_full(io, "hello", "worle", 5);
  if (error_code < 0) {
    std::cerr << "failed to rados_write_full: error_code " << error_code << std::endl;
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    return -1;
  }

  char buffer[1024] = {0};
  rados_ioctx_snap_set_read(io, LIBRADOS_SNAP_HEAD);
  int num_bytes = rados_read(io, "hello", buffer, 1024, uint64_t(0));
  if (num_bytes < 0) {
    std::cerr << "failed to rados_read: error_code " << num_bytes << std::endl;	  
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    return -1;
  }

  std::cout << "content " << buffer << " num_bytes " << num_bytes << std::endl;

  // read snapshot
  rados_ioctx_snap_set_read(io, snapid2);
  num_bytes = rados_read(io, "hello", buffer, 1024, uint64_t(0));
  if (num_bytes < 0) {
    std::cerr << "failed to rados_read: error_code " << num_bytes << std::endl;	  
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
    return -1;
  }

  std::cout << "content " << buffer << " num_bytes " << num_bytes << std::endl;


  rados_ioctx_destroy(io);
  rados_shutdown(cluster);

  return 0;
}
