/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <cassert>

// must before include spdlog.h
#ifdef BUILD_DEBUG
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_DEBUG
#else
#  define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_INFO
#endif

#if defined(__clang__)
#  define CLANG_VERSION (__clang_major__ * 100 + __clang_minor__)
#else
#  define CLANG_VERSION 0
#endif

#if CLANG_VERSION
// prevent the error "'codecvt<char32_t, char, __mbstate_t>' is deprecated" in C++20
#  pragma clang diagnostic ignored "-Wdeprecated"
#endif

#include "spdlog/sinks/daily_file_sink.h"
#include "spdlog/spdlog.h"

#ifdef BUILD_DEBUG
#  include "spdlog/sinks/stdout_sinks.h"
#endif

namespace logger {

class Logger {
 public:
  Logger() : log_name_("pikiwidb_log") {}

  static Logger& Instance() {
    static Logger log;
    return log;
  }

  void Init(const char* filepath) {
    std::vector<spdlog::sink_ptr> sinks;
    auto rotateSink = std::make_shared<spdlog::sinks::daily_file_sink_mt>(filepath, 2, 0, false, log_files_);
    sinks.push_back(rotateSink);
#ifdef BUILD_DEBUG
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_mt>());
#endif

    auto logger = std::make_shared<spdlog::logger>(log_name_, std::begin(sinks), std::end(sinks));
    const char* pattern = "[%Y-%m-%d %H:%M:%S.%e #%t %s:%# %l] %v";
    logger->set_pattern(pattern);
    spdlog::register_logger(logger);
    // spdlog::flush_every(std::chrono::seconds(3));
  }

  void SetLogFiles(int nfiles) {
    assert(nfiles > 0 && nfiles < 1000);
    log_files_ = nfiles;
  }

  const char* Name() const { return log_name_; }

 private:
  int log_files_ = 7;  // for a week

  const char* const log_name_;
};

inline void SetLogFiles(int nfiles) { logger::Logger::Instance().SetLogFiles(nfiles); }

inline void Init(const char* filepath) { logger::Logger::Instance().Init(filepath); }

}  // end namespace logger

#undef DEBUG
#undef INFO
#undef WARN
#undef ERROR

#define DEBUG(...) SPDLOG_LOGGER_DEBUG(spdlog::get(logger::Logger::Instance().Name()), __VA_ARGS__)
#define INFO(...) SPDLOG_LOGGER_INFO(spdlog::get(logger::Logger::Instance().Name()), __VA_ARGS__)
#define WARN(...) SPDLOG_LOGGER_WARN(spdlog::get(logger::Logger::Instance().Name()), __VA_ARGS__)
#define ERROR(...) SPDLOG_LOGGER_ERROR(spdlog::get(logger::Logger::Instance().Name()), __VA_ARGS__)
