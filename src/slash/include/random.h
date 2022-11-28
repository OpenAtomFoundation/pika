#ifndef SLASH_INCLUDE_RANDOM_H_
#define SLASH_INCLUDE_RANDOM_H_

#include <time.h>

namespace slash {

class Random {
 public:
  Random() {
    srand(time(NULL));
  }

  /*
   * return Random number in [1...n]
   */
  static uint32_t Uniform(int n) {
    return (random() % n) + 1;
  }

};

};  // namespace slash

#endif  // SLASH_INCLUDE_RANDOM_H_
