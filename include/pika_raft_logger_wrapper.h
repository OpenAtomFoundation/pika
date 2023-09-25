// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RAFT_PIKARAFTLOGGERWRAPPER_
#define PIKA_RAFT_PIKARAFTLOGGERWRAPPER_

#include <libnuraft/nuraft.hxx>
#include "pika_raft_logger.h"

using namespace nuraft;

/**
 * Example implementation of Raft logger, on top of SimpleLogger.
 */
class PikaRaftLoggerWrapper : public logger {
public:
    PikaRaftLoggerWrapper(const std::string& log_path, int log_level = 6) {
        my_log_ = new raft_logger::SimpleLogger(log_path + "pikaraft.log", 1024, 32*1024*1024, 10);
        my_log_->setLogLevel(log_level);
        my_log_->setDispLevel(-1);
        my_log_->setCrashDumpPath(log_path, true);
        my_log_->start();
    }

    ~PikaRaftLoggerWrapper() {
        destroy();
    }

    void destroy() {
        if (my_log_) {
            my_log_->flushAll();
            my_log_->stop();
            delete my_log_;
            my_log_ = nullptr;
        }
    }

    void put_details(int level,
                     const char* source_file,
                     const char* func_name,
                     size_t line_number,
                     const std::string& msg)
    {
        if (my_log_) {
            my_log_->put( level, source_file, func_name, line_number,
                          "%s", msg.c_str() );
        }
    }

    void set_level(int l) {
        if (!my_log_) return;

        if (l < 0) l = 1;
        if (l > 6) l = 6;
        my_log_->setLogLevel(l);
    }

    int get_level() {
        if (!my_log_) return 0;
        return my_log_->getLogLevel();
    }

    raft_logger::SimpleLogger* getLogger() const { return my_log_; }

private:
    raft_logger::SimpleLogger* my_log_;
};

#endif
