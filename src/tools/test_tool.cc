#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"

class WorkQueue : public ThreadPool::WorkQueueVal<std::string> {
public:        
  WorkQueue(string n, time_t ti, time_t sti, ThreadPool *p) : WorkQueueVal(n, ti, sti, p) {
  }

  ~WorkQueue() {
  }

private:  
  bool _empty() override {
    return queue_.empty();
  }

  void _enqueue(std::string item) override {
    queue_.push_back(item);
  }

  void _enqueue_front(std::string item) override {
    queue_.push_front(item);
  }

  bool _dequeue(std::string item) {
    ceph_abort();
  }

  std::string _dequeue() override {
    assert(!queue_.empty());
    std::string item = queue_.front();
    queue_.pop_front();
    return item;
  }

  void _process(std::string u, ThreadPool::TPHandle &) override {
    std::cout << "u " << u;
  }

  std::list<std::string> queue_;
};

int main(int argc, char* argv[]) {
  std::vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0); 
  common_init_finish(cct.get());

  ThreadPool tp(g_ceph_context, "tp_md5", "tp_md5", 8, "");
  WorkQueue work_queue("test", 0, 0, &tp);
  work_queue.queue("abc");

  tp.start();
  tp.drain(&work_queue);
  tp.stop();

  return 0;
}
