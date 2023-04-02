#include <iostream>

#include "pstd/include/pstd_mutex.h"

using namespace pstd;

int main() {
  Mutex mu;
  CondVar cv(&mu);

  mu.Lock();
  // sleep 1 millisecond
  cv.TimedWait(1);
  printf("sleep 1 millisecond\n");

  // sleep 1 second
  cv.TimedWait(1000);
  printf("sleep 1 second\n");
  cv.TimedWait(999);
  printf("sleep 999 millisecond\n");
  cv.TimedWait(999);
  printf("sleep 999 millisecond\n");
  cv.TimedWait(1);
  printf("sleep 1 millisecond\n");
  cv.TimedWait(999);
  printf("sleep 999 millisecond\n");
  cv.TimedWait(999);
  printf("sleep 999 millisecond\n");
  cv.TimedWait(999);
  printf("sleep 999 millisecond\n");
  cv.TimedWait(999);
  printf("sleep 999 millisecond\n");

  mu.Unlock();
  
  return 0;
}

