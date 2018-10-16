#include <iostream>
#include "mon/PGMap.h"

int main(int argc, char* argv[]) {
  PGMap pgmap;
  bufferlist bl;
  bufferlist::const_iterator p;
  std::string error_message;
  int error_code = bl.read_file(argv[1], &error_message);
  if (error_code < 0) {
    std::cerr << "failed to reaf from file: error_code " << error_code 
		<< " error_message " << error_message << std::endl;
    return -1;
  }
  p = bl.begin();
  pgmap.decode(p);
  std::cout << std::endl;
  JSONFormatter f = JSONFormatter(true);
  pgmap.dump(&f);
  f.flush(std::cout);
  std::cout << std::endl;

  return 0;
}
