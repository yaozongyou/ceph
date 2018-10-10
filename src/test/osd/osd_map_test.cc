#include <iostream>
#include "osd/OSDMap.h"
#include "mon/PGMap.h"

void calc_eval(OSDMap &osdmap) {
  set<int> roots;
  osdmap.crush->find_takes(&roots);

  std::cout << "roots " << roots << std::endl;
  for (auto rootid : roots) {
    std::cout << "rootid " << rootid << std::endl;	  
	//osdmap.get_pools_by_take(rootid);
	//
	map<int,float> pmap;
    osdmap.crush->get_take_weight_osd_map(rootid, &pmap);
	std::cout << "pmap " << pmap << std::endl;
  }

}

int main(int argc, char* argv[]) {
  std::cout << "Hello World!" << std::endl;
  std::string error_message;
  OSDMap osdmap;
  bufferlist bl;
  int error_code = bl.read_file("./osdmap_bin.txt", &error_message);
  if (error_code < 0) {
	std::cerr << "failed to reaf from file: error_code " << error_code 
		<< " error_message " << error_message << std::endl;
    return -1;
  }

  osdmap.decode(bl);
  std::cout << "osdmap " << osdmap << std::endl;
  osdmap.print_pools(std::cout);

  std::cout << std::endl;
  JSONFormatter f = JSONFormatter(true);
  osdmap.dump(&f);
  f.flush(std::cout);
  std::cout << std::endl;


  /*
  std::cout << std::endl;
  JSONFormatter ff = JSONFormatter(true);
  osdmap.crush->dump(&ff);
  ff.flush(std::cout);
  std::cout << std::endl;
	*/

  /*
  set<int> roots;
  osdmap.crush->find_takes(&roots);
  std::cout << "roots " << roots << std::endl;
  */

  pg_t pg;
  pg.parse("1.0");
  vector<int> up;
  vector<int> acting;
  osdmap.pg_to_up_acting_osds(pg, up, acting);
  //std::cout << "fsid " << osdmap.
  std::cout << "up " << up << " acting " << acting << std::endl;


  {
    PGMap  pgmap;
    bufferlist bl;
	bufferlist::const_iterator p;
    int error_code = bl.read_file("./pgmap_bin.txt", &error_message);
    if (error_code < 0) {
  	  std::cerr << "failed to reaf from file: error_code " << error_code 
  		  << " error_message " << error_message << std::endl;
      return -1;
    }
	p = bl.begin();
    pgmap.decode(p);

	/*
	std::cout << std::endl;
    JSONFormatter f = JSONFormatter(true);
	pgmap.dump(&f);
	f.flush(std::cout);
	std::cout << std::endl;
	*/
  }

  calc_eval(osdmap);

  return 0;
}
