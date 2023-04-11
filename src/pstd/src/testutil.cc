#include "pstd/include/testutil.h"

#include <string>

#include "pstd/include/env.h"
#include "pstd/include/random.h"

namespace pstd {

std::string RandomString(const int len) {
  char buf[len];
  for (int i = 0; i < len; i++) {
    buf[i] = Random::Uniform('z' - 'a') + 'a';
  }
  return std::string(buf, len);
}

int GetTestDirectory(std::string* result) {
  const char* env = getenv("TEST_TMPDIR");
  if (env && env[0] != '\0') {
    *result = env;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "/tmp/pstdtest-%d", int(geteuid()));
    *result = buf;
  }
  return 0;
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != NULL ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

}  // namespace pstd
