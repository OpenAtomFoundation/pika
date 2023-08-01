#include "pstd/include/testutil.h"

#include <sys/time.h>
#include <unistd.h>

#include <string>

#include "pstd/include/random.h"

namespace pstd {

void current_time_str(char * str, size_t max_len)
{
  struct timeval tv;
  struct tm tmm;

  gettimeofday(&tv, nullptr);

  localtime_r(&(tv.tv_sec), &tmm);
  snprintf(str, max_len, "%04d-%02d-%02dT%02d:%02d:%02d.%06ld",
           tmm.tm_year + 1900,
           tmm.tm_mon+1,
           tmm.tm_mday,
           tmm.tm_hour,
           tmm.tm_min,
           tmm.tm_sec,
           tv.tv_usec);  // NOLINT cause different between macOS and ubuntu
}

int32_t GetTestDirectory(std::string* result) {
  const char* env = getenv("TEST_TMPDIR");
  if (env && env[0] != '\0') {
    *result = env;
  } else {
    char buf[100];
    snprintf(buf, sizeof(buf), "/tmp/pstdtest-%d", static_cast<int32_t>(geteuid()));
    *result = buf;
  }
  return 0;
}

}  // namespace pstd
