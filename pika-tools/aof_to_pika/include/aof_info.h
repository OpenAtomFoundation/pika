#ifndef AOF_INFO
#define AOF_INFO

#include <string>

#define AOF_LOG_DEBUG 1
#define AOF_LOG_TRACE 2
#define AOF_LOG_INFO 3
#define AOF_LOG_WARN 4
#define AOF_LOG_ERR 5
#define AOF_LOG_FATAL 6

#define LOG_FATAL(content) info_print(AOF_LOG_FATAL, content)
#define LOG_ERR(content) info_print(AOF_LOG_ERR, content)
#define LOG_WARN(content) info_print(AOF_LOG_WARN, content)
#define LOG_INFO(content) info_print(AOF_LOG_INFO, content)
#define LOG_TRACE(content) info_print(AOF_LOG_TRACE, content)
#define LOG_DEBUG(content) info_print(AOF_LOG_DEBUG, content)

void set_info_level(int);
void info_print(int, const std::string&);
#endif
