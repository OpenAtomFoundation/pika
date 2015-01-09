#ifndef __CONSUME_H__
#define __CONSUME_H__

#include <string>

/**
 * @return int
 * @retval 0 meaning boolean false
 * @retval 1 meaning boolean true
 * @retval 2 meaning stopping consuming
 */
typedef int (*consume_fn)(const std::string&);

#define DEFAULT_CONF_PATH \
    "/home/infra/qbus/client/cpp/conf/qbus-client.conf"

int startConsumer(std::string &zk_cluster, std::string &topic, std::string &group,
        consume_fn fn, const std::string& conf_path = DEFAULT_CONF_PATH);

#endif // __CONSUME_H__
