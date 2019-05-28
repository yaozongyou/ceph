#include "common/log_message.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "os/ObjectStore.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_bluestore

int main(int argc, const char *argv[]) {
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  auto cct = global_init(nullptr, args, 
      CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  std::string osd_objectstore = "filestore";
  std::string osd_data = "./filestore-bench";
  std::string osd_journal = "./filestore-bench/journal";

  auto os = std::unique_ptr<ObjectStore>(ObjectStore::create(g_ceph_context, osd_objectstore, osd_data, osd_journal));
  if (!os) {
    //derr << "bad objectstore type " << g_conf->osd_objectstore << dendl;
    return 1;
  }

  if (os->mkfs() < 0) {
    derr << "mkfs failed" << dendl;
    return 1;
  }
  if (os->mount() < 0) {
    derr << "mount failed" << dendl;
    return 1;
  }

  os->umount();

  return 0;
}
