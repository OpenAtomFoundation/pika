/**
 * @file xdebug.h
 * @brief debug macros
 * @author chenzongzhi
 * @version 1.0.0
 * @date 2014-04-25
 */

#ifndef __XDEBUG_H__
#  define __XDEBUG_H__
#  include <errno.h>
#  include <stdio.h>
#  include <stdlib.h>
#  include <string.h>
#  include <unistd.h>

#  ifdef __XDEBUG__
#    define pint(x) qf_debug("%s = %d", #x, x)
#    define psize(x) qf_debug("%s = %zu", #x, x)
#    define pstr(x) qf_debug("%s = %s", #x, x)
// 如果A 不对, 那么就输出M
#    define qf_check(A, M, ...)    \
      if (!(A)) {                  \
        log_err(M, ##__VA_ARGS__); \
        errno = 0;                 \
        exit(-1);                  \
      }

// 用来检测程序是否执行到这里
#    define sentinel(M, ...)        \
      {                             \
        qf_debug(M, ##__VA_ARGS__); \
        errno = 0;                  \
      }

#    define qf_bin_debug(buf, size) \
      { fwrite(buf, 1, size, stderr); }

#    define _debug_time_def timeval s1, e;
#    define _debug_getstart gettimeofday(&s1, NULL)
#    define _debug_getend gettimeofday(&e, NULL)
#    define _debug_time ((int)(((e.tv_sec - s1.tv_sec) * 1000 + (e.tv_usec - s1.tv_usec) / 1000)))

#    define clean_errno() (errno == 0 ? "None" : strerror(errno))
#    define log_err(M, ...)                                                                                      \
      {                                                                                                          \
        fprintf(stderr, "[ERROR] (%s:%d %s errno: %s) " M "\n", __FILE__, __LINE__, get_date_time().c_str(), clean_errno(), ##__VA_ARGS__); \
        exit(-1);                                                                                                \
      }
#    define log_warn(M, ...) \
      fprintf(stderr, "[WARN] (%s:%d: errno: %s) " M "\n", __FILE__, __LINE__, clean_errno(), ##__VA_ARGS__)
#    define log_info(M, ...) fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#  else

#    define pint(x) \
      {}
#    define pstr(x) \
      {}
#    define qf_bin_debug(buf, size) \
      {}

#    define _debug_time_def \
      {}
#    define _debug_getstart \
      {}
#    define _debug_getend \
      {}
#    define _debug_time 0

#    define sentinel(M, ...) \
      {}
#    define qf_check(A, M, ...) \
      {}
#    define log_err(M, ...) \
      {}
#    define log_warn(M, ...) \
      {}
#    define log_info(M, ...) \
      {}

#  endif

#endif  //__XDEBUG_H__

/* vim: set ts=4 sw=4 sts=4 tw=100 */
