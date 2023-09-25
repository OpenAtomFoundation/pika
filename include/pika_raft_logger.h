// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RAFT_LOGGER_
#define PIKA_RAFT_LOGGER_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <list>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <signal.h>
#include <stdarg.h>
#if defined(__linux__) || defined(__APPLE__)
    #include <sys/time.h>
#endif

namespace raft_logger {

// To suppress false alarms by thread sanitizer,
// add -DSUPPRESS_TSAN_FALSE_ALARMS=1 flag to CXXFLAGS.
// #define SUPPRESS_TSAN_FALSE_ALARMS (1)

// 0: System  [====]
// 1: Fatal   [FATL]
// 2: Error   [ERRO]
// 3: Warning [WARN]
// 4: Info    [INFO]
// 5: Debug   [DEBG]
// 6: Trace   [TRAC]


// printf style log macro
#define _log_(level, l, ...)        \
    if (l && l->getLogLevel() >= level) \
        (l)->put(level, __FILE__, __func__, __LINE__, __VA_ARGS__)

#define _log_sys(l, ...)    _log_(SimpleLogger::SYS,     l, __VA_ARGS__)
#define _log_fatal(l, ...)  _log_(SimpleLogger::FATAL,   l, __VA_ARGS__)
#define _log_err(l, ...)    _log_(SimpleLogger::ERROR,   l, __VA_ARGS__)
#define _log_warn(l, ...)   _log_(SimpleLogger::WARNING, l, __VA_ARGS__)
#define _log_info(l, ...)   _log_(SimpleLogger::INFO,    l, __VA_ARGS__)
#define _log_debug(l, ...)  _log_(SimpleLogger::DEBUG,   l, __VA_ARGS__)
#define _log_trace(l, ...)  _log_(SimpleLogger::TRACE,   l, __VA_ARGS__)


// stream log macro
#define _stream_(level, l)              \
    if (l && l->getLogLevel() >= level) \
        l->eos() = l->stream(level, l, __FILE__, __func__, __LINE__)

#define _s_sys(l)   _stream_(SimpleLogger::SYS,     l)
#define _s_fatal(l) _stream_(SimpleLogger::FATAL,   l)
#define _s_err(l)   _stream_(SimpleLogger::ERROR,   l)
#define _s_warn(l)  _stream_(SimpleLogger::WARNING, l)
#define _s_info(l)  _stream_(SimpleLogger::INFO,    l)
#define _s_debug(l) _stream_(SimpleLogger::DEBUG,   l)
#define _s_trace(l) _stream_(SimpleLogger::TRACE,   l)


// Do printf style log, but print logs in `lv1` level during normal time,
// once in given `interval_ms` interval, print a log in `lv2` level.
// The very first log will be printed in `lv2` level.
//
// This function is global throughout the process, so that
// multiple threads will share the interval.
#define _timed_log_g(l, interval_ms, lv1, lv2, ...)                     \
{                                                                       \
    _timed_log_definition(static);                                      \
    _timed_log_body(l, interval_ms, lv1, lv2, __VA_ARGS__);             \
}

// Same as `_timed_log_g` but per-thread level.
#define _timed_log_t(l, interval_ms, lv1, lv2, ...)                     \
{                                                                       \
    _timed_log_definition(thread_local);                                \
    _timed_log_body(l, interval_ms, lv1, lv2, __VA_ARGS__);             \
}

#define _timed_log_definition(prefix)                                   \
    prefix std::mutex timer_lock;                                       \
    prefix bool first_event_fired = false;                              \
    prefix std::chrono::system_clock::time_point last_timeout =         \
        std::chrono::system_clock::now();                               \

#define _timed_log_body(l, interval_ms, lv1, lv2, ...)                  \
    std::chrono::system_clock::time_point cur =                         \
        std::chrono::system_clock::now();                               \
    bool timeout = false;                                               \
    {   std::lock_guard<std::mutex> l(timer_lock);                      \
        std::chrono::duration<double> elapsed = cur - last_timeout;     \
        if ( elapsed.count() * 1000 > interval_ms ||                    \
             !first_event_fired ) {                                     \
            cur = std::chrono::system_clock::now();                     \
            elapsed = cur - last_timeout;                               \
            if ( elapsed.count() * 1000 > interval_ms ||                \
                 !first_event_fired ) {                                 \
                timeout = first_event_fired = true;                     \
                last_timeout = cur;                                     \
            }                                                           \
        }                                                               \
    }                                                                   \
    if (timeout) {                                                      \
        _log_(lv2, l, __VA_ARGS__);                                     \
    } else {                                                            \
        _log_(lv1, l, __VA_ARGS__);                                     \
    }


class SimpleLoggerMgr;
class SimpleLogger {
    friend class SimpleLoggerMgr;
public:
    static const int MSG_SIZE = 4096;
    static const std::memory_order MOR = std::memory_order_relaxed;

    enum Levels {
        SYS         = 0,
        FATAL       = 1,
        ERROR       = 2,
        WARNING     = 3,
        INFO        = 4,
        DEBUG       = 5,
        TRACE       = 6,
        UNKNOWN     = 99,
    };

    class LoggerStream : public std::ostream {
    public:
        LoggerStream() : std::ostream(&buf), level(0), logger(nullptr)
                       , file(nullptr), func(nullptr), line(0) {}

        template<typename T>
        inline LoggerStream& operator<<(const T& data) {
            sStream << data;
            return *this;
        }

        using MyCout = std::basic_ostream< char, std::char_traits<char> >;
        typedef MyCout& (*EndlFunc)(MyCout&);
        inline LoggerStream& operator<<(EndlFunc func) {
            func(sStream);
            return *this;
        }

        inline void put() {
            if (logger) {
                logger->put( level, file, func, line,
                             "%s", sStream.str().c_str() );
            }
        }

        inline void setLogInfo(int _level,
                               SimpleLogger* _logger,
                               const char* _file,
                               const char* _func,
                               size_t _line)
        {
            sStream.str(std::string());
            level = _level;
            logger = _logger;
            file = _file;
            func = _func;
            line = _line;
        }

    private:
        std::stringbuf buf;
        std::stringstream sStream;
        int level;
        SimpleLogger* logger;
        const char* file;
        const char* func;
        size_t line;
    };

    class EndOfStmt {
    public:
        EndOfStmt() {}
        EndOfStmt(LoggerStream& src) { src.put(); }
        EndOfStmt& operator=(LoggerStream& src) { src.put(); return *this; }
    };

    LoggerStream& stream( int level,
                          SimpleLogger* logger,
                          const char* file,
                          const char* func,
                          size_t line ) {
        thread_local LoggerStream msg;
        msg.setLogInfo(level, logger, file, func, line);
        return msg;
    }

    EndOfStmt& eos() {
        thread_local EndOfStmt _eos;
        return _eos;
    }

private:
    struct LogElem {
        enum Status {
            CLEAN       = 0,
            WRITING     = 1,
            DIRTY       = 2,
            FLUSHING    = 3,
        };

        LogElem();

        // True if dirty.
        bool needToFlush();

        // True if no other thread is working on it.
        bool available();

        int write(size_t _len, char* msg);
        int flush(std::ofstream& fs);

        size_t len;
        char ctx[MSG_SIZE];
        std::atomic<Status> status;
    };

public:
    SimpleLogger(const std::string& file_path,
                 size_t max_log_elems           = 4096,
                 uint64_t log_file_size_limit   = 32*1024*1024,
                 uint32_t max_log_files         = 16);
    ~SimpleLogger();

    static void setCriticalInfo(const std::string& info_str);
    static void setCrashDumpPath(const std::string& path,
                                 bool origin_only = true);
    static void setStackTraceOriginOnly(bool origin_only);
    static void logStackBacktrace();

    static void shutdown();
    static std::string replaceString(const std::string& src_str,
                                     const std::string& before,
                                     const std::string& after);

    int start();
    int stop();

    inline bool traceAllowed() const { return (curLogLevel.load(MOR) >= 6); }
    inline bool debugAllowed() const { return (curLogLevel.load(MOR) >= 5); }

    void setLogLevel(int level);
    void setDispLevel(int level);
    void setMaxLogFiles(size_t max_log_files);

    inline int getLogLevel()  const { return curLogLevel.load(MOR); }
    inline int getDispLevel() const { return curDispLevel.load(MOR); }

    void put(int level,
             const char* source_file,
             const char* func_name,
             size_t line_number,
             const char* format,
             ...);
    void flushAll();

private:
    void calcTzGap();
    void findMinMaxRevNum(size_t& min_revnum_out,
                          size_t& max_revnum_out);
    void findMinMaxRevNumInternal(bool& min_revnum_initialized,
                                  size_t& min_revnum,
                                  size_t& max_revnum,
                                  std::string& f_name);
    std::string getLogFilePath(size_t file_num) const;
    void execCmd(const std::string& cmd);
    void doCompression(size_t file_num);
    bool flush(size_t start_pos);

    std::string filePath;
    size_t minRevnum;
    size_t curRevnum;
    std::atomic<size_t> maxLogFiles;
    std::ofstream fs;

    uint64_t maxLogFileSize;
    std::atomic<uint32_t> numCompJobs;

    // Log up to `curLogLevel`, default: 6.
    // Disable: -1.
    std::atomic<int> curLogLevel;

    // Display (print out on terminal) up to `curDispLevel`,
    // default: 4 (do not print debug and trace).
    // Disable: -1.
    std::atomic<int> curDispLevel;

    std::mutex displayLock;

    int tzGap;
    std::atomic<uint64_t> cursor;
    std::vector<LogElem> logs;
    std::mutex flushingLogs;
};

// Singleton class
class SimpleLoggerMgr {
public:
    struct CompElem;

    struct TimeInfo {
        TimeInfo(std::tm* src);
        TimeInfo(std::chrono::system_clock::time_point now);
        int year;
        int month;
        int day;
        int hour;
        int min;
        int sec;
        int msec;
        int usec;
    };

    struct RawStackInfo {
        RawStackInfo() : tidHash(0), kernelTid(0), crashOrigin(false) {}
        uint32_t tidHash;
        uint64_t kernelTid;
        std::vector<void*> stackPtrs;
        bool crashOrigin;
    };

    static SimpleLoggerMgr* init();
    static SimpleLoggerMgr* get();
    static SimpleLoggerMgr* getWithoutInit();
    static void destroy();
    static int getTzGap();
    static void handleSegFault(int sig);
    static void handleSegAbort(int sig);
#if defined(__linux__) || defined(__APPLE__)
    static void handleStackTrace(int sig, siginfo_t* info, void* secret);
#endif
    static void flushWorker();
    static void compressWorker();

    void logStackBacktrace(size_t timeout_ms = 60*1000);
    void flushCriticalInfo();
    void enableOnlyOneDisplayer();
    void flushAllLoggers() { flushAllLoggers(0, std::string()); }
    void flushAllLoggers(int level, const std::string& msg);
    void addLogger(SimpleLogger* logger);
    void removeLogger(SimpleLogger* logger);
    void addThread(uint64_t tid);
    void removeThread(uint64_t tid);
    void addCompElem(SimpleLoggerMgr::CompElem* elem);
    void sleepFlusher(size_t ms);
    void sleepCompressor(size_t ms);
    bool chkTermination() const;
    void setCriticalInfo(const std::string& info_str);
    void setCrashDumpPath(const std::string& path,
                          bool origin_only);
    void setStackTraceOriginOnly(bool origin_only);

    /**
     * Set the flag regarding exiting on crash.
     * If flag is `true`, custom segfault handler will not invoke
     * original handler so that process will terminate without
     * generating core dump.
     * The flag is `false` by default.
     *
     * @param exit_on_crash New flag value.
     * @return void.
     */
    void setExitOnCrash(bool exit_on_crash);

    const std::string& getCriticalInfo() const;

    static std::mutex displayLock;

private:
    // Copy is not allowed.
    SimpleLoggerMgr(const SimpleLoggerMgr&) = delete;
    SimpleLoggerMgr& operator=(const SimpleLoggerMgr&) = delete;

    static const size_t stackTraceBufferSize = 65536;

    // Singleton instance and lock.
    static std::atomic<SimpleLoggerMgr*> instance;
    static std::mutex instanceLock;

    SimpleLoggerMgr();
    ~SimpleLoggerMgr();

    void _flushStackTraceBuffer(size_t buffer_len,
                                uint32_t tid_hash,
                                uint64_t kernel_tid,
                                bool crash_origin);
    void flushStackTraceBuffer(RawStackInfo& stack_info);
    void flushRawStack(RawStackInfo& stack_info);
    void addRawStackInfo(bool crash_origin = false);
    void logStackBackTraceOtherThreads();

    bool chkExitOnCrash();

    std::mutex loggersLock;
    std::unordered_set<SimpleLogger*> loggers;

    std::mutex activeThreadsLock;
    std::unordered_set<uint64_t> activeThreads;

    // Periodic log flushing thread.
    std::thread tFlush;

    // Old log file compression thread.
    std::thread tCompress;

    // List of files to be compressed.
    std::list<CompElem*> pendingCompElems;

    // Lock for `pendingCompFiles`.
    std::mutex pendingCompElemsLock;

    // Condition variable for BG flusher.
    std::condition_variable cvFlusher;
    std::mutex cvFlusherLock;

    // Condition variable for BG compressor.
    std::condition_variable cvCompressor;
    std::mutex cvCompressorLock;

    // Termination signal.
    std::atomic<bool> termination;

    // Original segfault handler.
    void (*oldSigSegvHandler)(int);

    // Original abort handler.
    void (*oldSigAbortHandler)(int);

    // Critical info that will be displayed on crash.
    std::string globalCriticalInfo;

    // Reserve some buffer for stack trace.
    char* stackTraceBuffer;

    // TID of thread where crash happens.
    std::atomic<uint64_t> crashOriginThread;

    std::string crashDumpPath;
    std::ofstream crashDumpFile;

    // If `true`, generate stack trace only for the origin thread.
    // Default: `true`.
    bool crashDumpOriginOnly;

    // If `true`, do not invoke original segfault handler
    // so that process just terminates.
    // Default: `false`.
    bool exitOnCrash;

    std::atomic<uint64_t> abortTimer;

    // Assume that only one thread is updating this.
    std::vector<RawStackInfo> crashDumpThreadStacks;
};

}

#endif