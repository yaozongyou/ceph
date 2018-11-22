// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_bench.h"
#include <getopt.h>
#include <string.h>
#include <iostream>
#include "global/global_init.h"

static bool g_show_usage = false;
std::string g_bench_type;
int g_bench_concurrent;
int g_bench_secs;

static void ShowUsage() {
  std::cout << "radosgw-bench [options] 127.0.0.1:8000" << std::endl
      << "  -s, --secs                       bench time  [60]" << std::endl
      << "  -t, --type                       write or read [write]" << std::endl
      << "  -c, --concurrent                 bench concurrent [8]" << std::endl
      << "  -f, --format                     output format [plain]" << std::endl
      << "  --access-key                     access key" << std::endl
      << "  --secret-key                     secret key" << std::endl
      << "  -h, --help                       Show this help" << std::endl
      << "example:" << std::endl
      << "  radosgw-bench 127.0.0.1:8000" << std::endl;
}

int main(int argc, const char* argv[]) {
  RGWBench::Config bench_config;

  while (1) {
    int option_index = 0;
    static struct option long_options[] = {
      {"secs",        1, 0, 's' },
      {"type",        1, 0, 't' },
      {"concurrent",  1, 0, 'c' },
      {"object-size", required_argument, 0, 0   },
      {"access-key",  1, 0, '1' },
      {"secret-key",  1, 0, '2' },
      {"help",        0, 0, 'h' },
      {0,             0, 0, 0   }
    };

    int c = getopt_long(argc, const_cast<char * const*>(argv), "s:t:c:?h1:2:", long_options, &option_index);
    if (c == -1) {
      break;
    }

    switch (c) {
    case 0:
      if (strcmp(long_options[option_index].name, "object-size") == 0) {
	bench_config.object_size = atoi(optarg);
      }
      break;
    case 's':
      g_bench_secs = atoi(optarg);
      break;
    case 't':
      g_bench_type = optarg;
    case 'c':
      g_bench_concurrent = atoi(optarg);
    case '1':
      bench_config.access_key = optarg;
    case '2':
      bench_config.secret_key = optarg;
    case '?':
      // follow through
    case 'h':
      g_show_usage = true;
      break;
    default:
      printf ("?? getopt returned character code 0%o ??\n", c);
    }
  }

  if (optind != (argc - 1)) {
    ShowUsage();
    return -1;
  }

  if (g_show_usage) {
    ShowUsage();
    return 0;
  }

  vector<const char*> args;
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);

  bench_config.rgw_address = argv[argc -1];
  std::cout << "begin to bench using " << bench_config.rgw_address << std::endl;

  RGWBench bench(&*cct, bench_config);
  bench.prepare();
  bench.execute();
  bench.cleanup();

  return 0;
}
