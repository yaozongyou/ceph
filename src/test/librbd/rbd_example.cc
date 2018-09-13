#include <iostream>
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

int main() {
  librados::Rados rados;

  int error_code = rados.init("admin"); 
  if (error_code != 0) {
    std::cerr << "failed to init rados: error_code " << error_code << std::endl;
    return -1;	  
  }

  error_code = rados.conf_read_file("./ceph.conf");
  if (error_code != 0) {
    std::cerr << "failed to read conf file: error_code " << error_code << std::endl;
    return -1;	  
  }

  error_code = rados.connect();
  if (error_code != 0) {
    std::cerr << "failed to connect: error_code " << error_code << std::endl;	
    return -1;
  }

  librados::IoCtx io_ctx; 
  error_code = rados.ioctx_create("rbd", io_ctx);
  if (error_code != 0) {
    std::cerr << "failed to create ioctx: error_code " << error_code << std::endl;	
    return -1;	  
  }

  librbd::RBD rbd;
  librbd::Image image;

  error_code = rbd.open(io_ctx, image, "imagea");
  if (error_code != 0) {
    std::cerr << "failed to open image: error_code " << error_code << std::endl;	
    return -1;	  
  }

  ceph::bufferlist bl;
  bl.append("123");
  ssize_t ret = image.write(0, 3, bl);
  if (ret != 3) {
    std::cerr << "failed to write image: error_code " << error_code << std::endl;	
    return -1;
  }

  error_code = image.flush();
  if (error_code != 0) {
    std::cerr << "failed to flush image: error_code " << error_code << std::endl;	
    return -1;	  
  }

  error_code = image.close(); 
  if (error_code != 0) {
    std::cerr << "failed to close image: error_code " << error_code << std::endl;	
    return -1;	  
  }

  rados.shutdown();
  return 0;

  /*	
  rados_t _cluster;
  rados_ioctx_t ioctx;
  rbd_image_t image;
    
  ASSERT_EQ("", connect_cluster(&_cluster));
  
  rados_ioctx_t ioctx;
  ASSERT_EQ(0, rados_ioctx_create(_cluster, m_pool_name.c_str(), &ioctx));

  rbd_open(io, "imagea", &image, nullptr);

  rbd_completion_t c;
  rbd_aio_write(image, 0, 1, "a", c);

  rados_shutdown(_cluster);
  */

  return 0;
}
