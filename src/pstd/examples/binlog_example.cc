#include <iostream>

#include "pstd/include/pstd_binlog.h"
#include "pstd/include/xdebug.h"

using namespace pstd;

Binlog* log;

#define UNUSED(arg) ((void)arg)

void* Append(void* arg) {
  UNUSED(arg);
  int i = 0;
  while (true) {
    std::string item = "item" + std::to_string(i++);
    Status s = log->Append(item);
    printf("+ Append (%s) return %s\n", item.c_str(), s.ToString().c_str());
    sleep(1);
  }
  return NULL;
}

void* Reader(void* arg) {
  UNUSED(arg);
  int* id = (int*)arg;
  BinlogReader* reader = log->NewBinlogReader(0, 0);

  while (true) {
    std::string str;
    Status s = reader->ReadRecord(str);
    if (s.ok()) {
      printf("- Reader %u: get (%s)\n", *id, str.c_str());
    } else {
      printf("- Reader %lu: %s\n", pthread_self(), s.ToString().c_str());
    }
    sleep(1);
  }
  return NULL;
}

int main() {
  Status s = Binlog::Open("./binlog", &log);
  if (!s.ok()) {
    printf("Open failed %s\n", s.ToString().c_str());
    return -1;
  }

  pthread_t pid;
  pthread_create(&pid, NULL, &Append, NULL);

  pthread_t cid[3];
  int id[3];

  for (int i = 0; i < 3; i++) {
    id[i] = i;
    pthread_create(&cid[i], NULL, &Reader, &id[i]);
  }

  void* res;
  pthread_join(pid, &res);

  return 0;
}
