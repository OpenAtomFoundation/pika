#include <iostream>

#include "pstd/include/cond_lock.h"

#include <unistd.h>

pstd::CondLock cl;

int main()
{
  cl.Lock();
  cl.Unlock();
  uint32_t a = 2000;
  cl.Lock();
  cl.TimedWait(a);
  return 0;
}
