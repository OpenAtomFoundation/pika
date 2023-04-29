#ifndef __PSTD_INCLUDE_RANDOM_H__
#define __PSTD_INCLUDE_RANDOM_H__

#include <time.h>

namespace pstd {

class Random {
 public:
  Random() { srand(time(nullptr)); }

  /*
   * return Random number in [1...n]
   */
  static uint32_t Uniform(int n) { return (random() % n) + 1; }
};

};  // namespace pstd

#endif  // __PSTD_INCLUDE_RANDOM_H__
