#include "pstd/include/testutil.h"

#include <sys/time.h>
#include <string>

#include "pstd/include/random.h"

namespace pstd {

void current_time_str(char * str, size_t max_len)
{
  struct timeval tv;
  struct tm tmm;

  gettimeofday(&tv, 0);

  localtime_r(&(tv.tv_sec), &tmm);
  snprintf(str, max_len, "%04d-%02d-%02dT%02d:%02d:%02d.%06d",
           tmm.tm_year + 1900,
           tmm.tm_mon+1,
           tmm.tm_mday,
           tmm.tm_hour,
           tmm.tm_min,
           tmm.tm_sec,
           tv.tv_usec);
}

}  // namespace pstd
