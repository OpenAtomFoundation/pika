#ifndef _CONF_H
#define _CONF_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif


#include "HSearchHelper.h"

#include <stdlib.h>
#include <pthread.h>

#include <string>
#include <fstream>


class Conf {
private:
    static struct hsearch_data config_;
    static pthread_rwlock_t config_lock_;
    static bool loaded_;

    // inline
    static void clean(std::fstream *fsp = NULL)
    {
        if (fsp != NULL) fsp->close();

        if (config_.filled > 0) {
            for (unsigned int i = 0; i <= config_.size; i++) {
                if (config_.table[i].used != 0
                    && config_.table[i].used != (unsigned int)-1) {
                    // XXX
                    // key & data are new-ed when ENTER
                    // so it's rational here
                    delete config_.table[i].entry.key;
                    delete [] (char *)config_.table[i].entry.data;
                }
            }
        }

        hdestroy_r(&config_);

        pthread_rwlock_unlock(&config_lock_);
    }

public:
    static int loadConf(const std::string& path);
    static std::string readConf(const std::string& key,
                                const std::string& defaultValue);
    static int readConf(const std::string& key, int defaultValue);
    static bool readConf(const std::string& key, bool defaultValue);
};


#endif
