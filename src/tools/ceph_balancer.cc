#include <algorithm>
#include <getopt.h>
#include <iostream>
#include <stdlib.h>
#include "crush/CrushWrapper.h"
#include "mon/PGMap.h"
#include "osd/OSDMap.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "stddev.h"

static bool g_show_usage = false;

// ceph osd df -f json-pretty | grep -E "\"kb\"|\"id\"" | sed 'N;s/\n/ /' | awk '{print $2, $4}' | tr -d ','
static void ShowUsage() {
  std::cout << "ceph_balancer osdmap.bin pgmap.bin [pool_name]" << std::endl
      << "  -h, --help                 Show this help" << std::endl
      << "  -c, --count                count" << std::endl
      << "example:" << std::endl
      << "  ceph_balancer osdmap.bin pgmap.bin testpool" << std::endl
      << std::endl
      << std::endl
	  << "note: " << std::endl
      << "  ceph osd getmap > osdmap.bin" << std::endl
      << "  ceph pg getmap -o ./pgmap.bin" << std::endl;
}

bool double_equals(double a, double b, double epsilon = 0.001)
{
    return std::abs(a - b) < epsilon;
}


int LoadOSDMapFromFile(const std::string& filepath, OSDMap* osdmap) {
  bufferlist bl; 
  std::string error_message;
  int error_code = bl.read_file(filepath.c_str(), &error_message);
  if (error_code < 0) {
    std::cerr << "failed to read_file: error_code " << error_code << std::endl;
    return -1;	  
  }

  try {
    osdmap->decode(bl);
  } catch (ceph::buffer::end_of_buffer &eob) {
    std::cerr << "Exception (end_of_buffer) in decode(), exit." << std::endl;
    return -1;
  }

  return 0;
}

int LoadOsdCapacity(const std::string& filepath, std::map<int, uint64_t>* osd_to_capacity) {
  osd_to_capacity->clear(); 

  for (int i = 0; i <= 179; i++ ) {
	  (*osd_to_capacity)[i] = 3885588792;
  }
  for (int i = 180; i <= 455; i++ ) {
	  (*osd_to_capacity)[i] = 7792408576;
  }

  return 0;
}

std::map<int, int> GetUpOSDToPGNumbers(OSDMap* osdmap, const std::string& pool_name) {
  std::map<int, int> up_osd_to_pg_numbers;

  if (!pool_name.empty()) {
    uint64_t poolid = osdmap->lookup_pg_pool_name(pool_name);
    uint32_t pg_num = osdmap->get_pg_num(poolid);
    for (uint32_t pgid = 0; pgid < pg_num; pgid++) {
      vector<int> up, acting;
      int up_p, acting_p;
      osdmap->pg_to_up_acting_osds(pg_t{pgid, poolid}, &up, &up_p, &acting, &acting_p);

      for (auto osd : up) {
        if (up_osd_to_pg_numbers.find(osd) == up_osd_to_pg_numbers.end()) {
          up_osd_to_pg_numbers[osd] = 1;
        } else {
          up_osd_to_pg_numbers[osd] = up_osd_to_pg_numbers[osd] + 1;
        }
      }
    }
  } else {
    uint64_t pool_max = osdmap->get_pool_max();
    for (uint64_t poolid = 1; poolid <= pool_max; poolid++) {
      if (!osdmap->have_pg_pool(poolid)) {
        continue;		
      }

      uint32_t pg_num = osdmap->get_pg_num(poolid);
      for (uint32_t pgid = 0; pgid < pg_num; pgid++) {
        vector<int> up, acting;
        int up_p, acting_p;
        osdmap->pg_to_up_acting_osds(pg_t{pgid, poolid}, &up, &up_p, &acting, &acting_p);

        for (auto osd : up) {
          if (up_osd_to_pg_numbers.find(osd) == up_osd_to_pg_numbers.end()) {
            up_osd_to_pg_numbers[osd] = 1;
          } else {
            up_osd_to_pg_numbers[osd] = up_osd_to_pg_numbers[osd] + 1;
          }
        }
      }
    }
  }

  return up_osd_to_pg_numbers;
}

void print(OSDMap* osdmap, const std::map<int, uint64_t>& osd_to_capacity, const std::string& pool_name) {
  std::map<int, int> up_osd_to_pg_numbers = GetUpOSDToPGNumbers(osdmap, pool_name);

  Stddev stddev;
  for (auto& [osd, pg_numbers] : up_osd_to_pg_numbers) {
    std::cout << "osd " << osd << " pg_numbers " << pg_numbers 
        << " total " << osd_to_capacity.at(osd)
        << " TiB " << osd_to_capacity.at(osd) * 1.0 / 1024 / 1024 / 1024
        << std::endl;

    double capacity = osd_to_capacity.at(osd) * 1.0 / 1024 / 1024 / 1024;
    stddev.enter(pg_numbers / capacity);
  }
  std::cout << "stddev " << stddev.value() << std::endl;
}

double evaluate(OSDMap* osdmap, const std::map<int, uint64_t>& osd_to_capacity, const std::string& pool_name) {
  std::map<int, int> up_osd_to_pg_numbers = GetUpOSDToPGNumbers(osdmap, pool_name);

  Stddev stddev;

  for (auto& [osd, pg_numbers] : up_osd_to_pg_numbers) {
    double capacity = osd_to_capacity.at(osd) * 1.0 / 1024 / 1024 / 1024;
    stddev.enter(pg_numbers / capacity);
  }

  return stddev.value();
}

int FindMinLoadOsd(OSDMap* osdmap, const std::map<int, uint64_t>& osd_to_capacity, const std::string& pool_name) {
  std::map<int, int> up_osd_to_pg_numbers = GetUpOSDToPGNumbers(osdmap, pool_name);
  map<int, double> osd_loads;

  for (auto& [osd, pg_numbers] : up_osd_to_pg_numbers) {
    double capacity = osd_to_capacity.at(osd) * 1.0 / 1024 / 1024 / 1024;
    double load = pg_numbers / capacity;
	osd_loads[osd] = load;
  }

  auto it = std::min_element(osd_loads.begin(), osd_loads.end(),
      [](decltype(osd_loads)::value_type& l, decltype(osd_loads)::value_type& r) -> bool { return l.second < r.second; });

  return it->first;
}

int FindMaxLoadOsd(OSDMap* osdmap, const std::map<int, uint64_t>& osd_to_capacity, const std::string& pool_name) {
  std::map<int, int> up_osd_to_pg_numbers = GetUpOSDToPGNumbers(osdmap, pool_name);
  map<int, double> osd_loads;

  for (auto& [osd, pg_numbers] : up_osd_to_pg_numbers) {
    double capacity = osd_to_capacity.at(osd) * 1.0 / 1024 / 1024 / 1024;
    double load = pg_numbers / capacity;
	osd_loads[osd] = load;
  }

  auto it = std::max_element(osd_loads.begin(), osd_loads.end(),
      [](decltype(osd_loads)::value_type& l, decltype(osd_loads)::value_type& r) -> bool { return l.second < r.second; });

  return it->first;
}

int main(int argc, char* argv[]) {
  int evaluate_count = 1;

  while (1) {
    int option_index = 0;
    static struct option long_options[] = {
      {"help", 0, 0, 'h' },
      {"count", required_argument, 0, 'c' },
      {0,      0, 0, 0   }
    };

    int c = getopt_long(argc, const_cast<char * const*>(argv), "?hc:", long_options, &option_index);
    if (c == -1) {
      break;
    }

    switch (c) {
    case 0:
      break;
    case '?':
      // follow through
    case 'h':
      g_show_usage = true;
      break;
	case 'c':
	  evaluate_count = std::atoi(optarg);
	  break;
    default:
      printf("?? getopt returned character code 0%o ??\n", c);
    }
  }

  if (g_show_usage) {
    ShowUsage();
    return 0;
  }

  if (argc < 3 || optind > (argc - 2)) {
    ShowUsage();
    return -1;
  }

  std::string osdmap_bin = argv[optind];
  //std::string pgmap_bin = argv[optind+1];
  std::string pool_name;
  if ((optind+2) < argc) {
    pool_name = argv[optind+2];
  }

  OSDMap osdmap;
  int32_t error_code = LoadOSDMapFromFile(osdmap_bin, &osdmap);
  if (error_code != 0) {
	std::cerr << "failed to LoadOSDMapFromFile" << std::endl;
    return -1;
  }


  std::map<int, uint64_t> osd_to_capacity;
  LoadOsdCapacity("", &osd_to_capacity);
  if (error_code != 0) {
	std::cerr << "failed to LoadOSDCapacity" << std::endl;
    return -1;
  }
    
  vector<const char *> empty_args;
  auto cct = global_init(NULL, empty_args, CEPH_ENTITY_TYPE_CLIENT,
                       CODE_ENVIRONMENT_UTILITY,
                       CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);	
    
  std::cout << "Before:" << std::endl;
  //print(&osdmap, osd_to_capacity, pool_name);
  std::cout << std::endl << std::endl;
  std::map<int, int> up_osd_to_pg_numbers = GetUpOSDToPGNumbers(&osdmap, pool_name);

  std::map<int, int> rules;  // osd -> crush weight

  std::cout << "evaluate_count " << evaluate_count << std::endl;

  for (int count = 0; count < evaluate_count; count++) {
	int result = 0;
    double stddev = evaluate(&osdmap, osd_to_capacity, pool_name);

    int min_load_osd = FindMinLoadOsd(&osdmap, osd_to_capacity, pool_name);
    std::cout << "min_load_osd " << min_load_osd << std::endl;
    int max_load_osd = FindMaxLoadOsd(&osdmap, osd_to_capacity, pool_name);
    std::cout << "max_load_osd " << max_load_osd << std::endl;

    int min_load_osd_weight = osdmap.crush->get_item_weight(min_load_osd);
    int max_load_osd_weight = osdmap.crush->get_item_weight(max_load_osd);

    for (int i = 64; i < 20000; i += 64) {
      osdmap.crush->adjust_item_weight(cct.get(), min_load_osd, min_load_osd_weight+i);
      osdmap.crush->adjust_item_weight(cct.get(), max_load_osd, max_load_osd_weight-i);

      double new_stddev = evaluate(&osdmap, osd_to_capacity, pool_name);

	  if (double_equals(new_stddev, stddev, 0.00001)) {
        continue;
  	  } else if (new_stddev < stddev) {
        std::cout << "ceph osd crush reweight osd." << min_load_osd << " " 
		    << (min_load_osd_weight+i) / (float)0x10000 << std::endl;
	    std::cout << "ceph osd crush reweight osd." << max_load_osd << " " 
		    << (max_load_osd_weight-i) / (float)0x10000 << std::endl;

        rules[min_load_osd] = min_load_osd_weight+i;
        rules[max_load_osd] = max_load_osd_weight-i;

        std::cout << "goooood" << std::endl;
        std::cout << "stddev " << stddev << " new_stddev " << new_stddev << std::endl;
		result = 1;
	    break;
	  } else {
	    std::cout << "ceph osd crush reweight osd." << min_load_osd << " " 
		    << (min_load_osd_weight+i) / (float)0x10000 << std::endl;
	    std::cout << "ceph osd crush reweight osd." << max_load_osd << " " 
		    << (max_load_osd_weight-i) / (float)0x10000 << std::endl;

        rules[min_load_osd] = min_load_osd_weight+i;
        rules[max_load_osd] = max_load_osd_weight-i;

        std::cout << "baaaaad" << std::endl;
        std::cout << "stddev " << stddev << " new_stddev " << new_stddev << std::endl;
		result = -1;
	    break;
	  }
    } 

	if (result <= 0) {
	  //break;
	}
  }
    
  std::cout << std::endl << std::endl << "After:" << std::endl;
  //print(&osdmap, osd_to_capacity, pool_name);

  std::cout << std::endl << std::endl << "Change:" << std::endl;
  std::map<int, int> new_up_osd_to_pg_numbers = GetUpOSDToPGNumbers(&osdmap, pool_name);
  for (auto& [osd, pg_numbers] : new_up_osd_to_pg_numbers) {
    int old_pg_numbers = up_osd_to_pg_numbers[osd];
    if (pg_numbers != old_pg_numbers) {
      std::cout << "osd." << osd << " " << old_pg_numbers << " -> " << pg_numbers << std::endl; 
    }
  }

 
  std::cout << std::endl << std::endl << "rules: " << std::endl;
  for (auto& [osd, weight] : rules) {
    std::cout << "ceph osd crush reweight osd." << osd << " " << (weight) / (float)0x10000 << std::endl;
  }

  return 0;
}
