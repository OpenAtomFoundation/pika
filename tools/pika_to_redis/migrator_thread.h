#ifndef MIGRATOR_THREAD_H_
#define MIGRATOR_THREAD_H_

#include "nemo.h"
#include "pink/include/redis_cli.h"
#include "parse_thread.h"

class MigratorThread : public pink::Thread {
 public:
  MigratorThread(nemo::Nemo *db, std::vector<ParseThread*> &parsers, char type) :
      db_(db),
      parsers_(parsers),
      type_(type),
      thread_index_(0),
      num_thread_(parsers.size()){
      }

  virtual ~ MigratorThread();
  bool should_exit_;
 private:
  nemo::Nemo *db_;
  std::vector<ParseThread*> parsers_;
  char type_;
  int thread_index_;
  int num_thread_;

  static std::string GetKey(const rocksdb::Iterator *it);
  void MigrateDB(char type);
  void DispatchKey(const std::string &key, char type);

  virtual void *ThreadMain();
};
#endif
