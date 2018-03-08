// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <cassert>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "os/ObjectStore.h"

#include "global/global_init.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/log_message.h"
#include "counter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_filestore

// use big pool ids to avoid clashing with existing collections
static constexpr int64_t POOL_ID = 0x0000ffffffffffff;
static constexpr uint32_t PG_NUMBER = 256;

COUNTER_frequency(write, "write frequency");
COUNTER_latency(write, "write latency");
COUNTER_distribution(write, "write distribution");

static void usage() {
  derr << "usage: ceph_objectstore_bench [flags]\n"
      "	 --threads\n"
      "	       number of threads to carry out this workload\n" << dendl;
  generic_server_usage();
}

struct Config {
  int threads{10};
};

class C_NotifyCond : public Context {
  std::mutex *mutex;
  std::condition_variable *cond;
  bool *done;
public:
  C_NotifyCond(std::mutex *mutex, std::condition_variable *cond, bool *done) : mutex(mutex), cond(cond), done(done) {}

  void finish(int r) override {
    std::lock_guard<std::mutex> lock(*mutex);
    *done = true;
    cond->notify_one();
  }
};

ObjectStore::CollectionHandle OpenCollection(ObjectStore* os, uint32_t i) {
  auto pg = spg_t{pg_t{i, POOL_ID}};
  coll_t cid(pg);
  return os->open_collection(cid);	  
}

void WriteObject(ObjectStore* os, ObjectStore::CollectionHandle ch, spg_t pg,
		 const std::string& object, const std::string& content) {
    common::ScopedLatency scoped_latency_write(&LATENCY_write);

    std::mutex mutex;
    std::condition_variable cond;
    bool done = false;

    ObjectStore::Transaction t;
    t.register_on_applied(new C_NotifyCond(&mutex, &cond, &done));
    bufferlist bl;
    bl.append(content);
    t.write(ch->get_cid(), 
            ghobject_t(hobject_t(object, "", CEPH_NOSNAP, pg.ps(), pg.pool(), "")), 
            0, bl.length(), bl);
    
    int r = os->queue_transaction(ch, std::move(t));
    assert(r == 0);

    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&done](){ return done; });
    lock.unlock();

    FREQUENCY_write.Add();
}

void OsbenchWorker(ObjectStore* os) {
  for (int i = 0; i < 1024; ++i) {	
    uint32_t j = random() % PG_NUMBER;	
    ObjectStore::CollectionHandle ch = OpenCollection(os, j);
    auto pg = spg_t{pg_t{j, POOL_ID}};	 
    uuid_d uuid;
    uuid.generate_random();

    uint32_t seed = random();
    std::string content;
    for (std::size_t k = 0; k < 2 * 1024 * 1024; k += sizeof(int32_t)) {
      int32_t random = rand_r(&seed);
      content.append((char*)(&random), sizeof(random));
    }   

    WriteObject(os, ch, pg, uuid.to_string(), content);
  }
}

int init_collections(std::unique_ptr<ObjectStore>& os,
		     uint64_t pool,
		     uint64_t count) {
  assert(count > 0);

  const int split_bits = cbits(count - 1);

  {
    // propagate Superblock object to ensure proper functioning of tools that
    // need it. E.g. ceph-objectstore-tool
    coll_t cid(coll_t::meta());
    bool exists = os->collection_exists(cid);
    if (!exists) {
      auto ch = os->create_new_collection(cid);	

      OSDSuperblock superblock;
      bufferlist bl;
      encode(superblock, bl);

      ObjectStore::Transaction t;
      t.create_collection(cid, split_bits);
      t.write(cid, OSD_SUPERBLOCK_GOBJECT, 0, bl.length(), bl);
      int r = os->queue_transaction(ch, std::move(t));

      if (r < 0) {
        derr << "Failure to write OSD superblock: " << cpp_strerror(-r) << dendl;
	return r;
      }
    }
  }

  for (uint32_t i = 0; i < count; i++) {
    auto pg = spg_t{pg_t{i, pool}};	 
    coll_t cid(pg);

    if (os->collection_exists(cid)) {
      continue;	    
    }

    std::mutex mutex;
    std::condition_variable cond;
    bool done = false;

    ObjectStore::Transaction t;
    t.create_collection(cid, split_bits);
    ghobject_t pgmeta_oid(pg.make_pgmeta_oid());
    t.touch(cid, pgmeta_oid);
    t.register_on_applied(new C_NotifyCond(&mutex, &cond, &done));
    auto ch = os->create_new_collection(cid);
    int r = os->queue_transaction(ch, std::move(t));
    assert(r == 0);

    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&done](){ return done; });
    lock.unlock();
  }

  return 0;
}

int main(int argc, const char *argv[]) {
  common::InitCounterManager();
  srand(time(NULL));	
  Config cfg;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  std::string val;
  vector<const char*>::iterator i = args.begin();
  while (i != args.end()) {
    if (ceph_argparse_double_dash(args, i))
      break;

    if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)nullptr)) {
      cfg.threads = atoi(val.c_str());
    } else {
      derr << "Error: can't understand argument: " << *i << "\n" << dendl;
      usage();
    }
  }

  common_init_finish(g_ceph_context);
  
  LOG(INFO) << "objectstore " << g_conf->osd_objectstore;
  LOG(INFO) << "data " << g_conf->osd_data;
  LOG(INFO) << "journal " << g_conf->osd_journal;
  LOG(INFO) << "threads " << cfg.threads;

  auto os = std::unique_ptr<ObjectStore>(
      ObjectStore::create(g_ceph_context,
                          g_conf->osd_objectstore,
                          g_conf->osd_data,
                          g_conf->osd_journal));

  if (!os) {
    derr << "bad objectstore type " << g_conf->osd_objectstore << dendl;
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
  
  init_collections(os, POOL_ID, PG_NUMBER);

  // run the worker threads
  std::vector<std::thread> workers;
  workers.reserve(cfg.threads);

  for (int i = 0; i < cfg.threads; i++) {
    workers.emplace_back(OsbenchWorker, os.get());
  }
  for (auto &worker : workers) {
    worker.join();
  }
  workers.clear();

  os->umount();
  common::ShutDownCounterManager();

  return 0;	
}
