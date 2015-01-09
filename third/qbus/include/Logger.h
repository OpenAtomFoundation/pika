#ifndef __KAFKALOGGER_H__
#define __KAFKALOGGER_H__

#include <sys/types.h>
#include <string>
#include <pthread.h>
#include <stdio.h>

#define LOG_ERR(format, ...)       KafkaLogger::getInstance()->error(__FILE__, __LINE__, format, ## __VA_ARGS__)
#define LOG_WARN(format, ...)      KafkaLogger::getInstance()->warn(__FILE__, __LINE__, format, ## __VA_ARGS__)
#define LOG_INFO(format, ...)      KafkaLogger::getInstance()->info(__FILE__, __LINE__, format, ## __VA_ARGS__)
#define LOG_TRACE(format, ...)     KafkaLogger::getInstance()->trace(__FILE__, __LINE__, format, ## __VA_ARGS__)
#define LOG_DEBUG(format, ...)     KafkaLogger::getInstance()->debug(__FILE__, __LINE__, format, ## __VA_ARGS__)

class KafkaLogger
{
    private:
        KafkaLogger();
        ~KafkaLogger();
        FILE* file_;
        int level_;
        std::string dir_;
        std::string logPrefix_;
        bool isMutexInit_;
        char curLogFile_[256];
        pthread_mutex_t mutex_;
        static KafkaLogger *instance_;

        void init();
        int checkLogFile(int year, int month, int day);
        void writeLog(char* file_name, int line_no, int level, const char* format, ...);
    public:
        static KafkaLogger* getInstance();
        void info(char* file_name, int line_no, const char* format, ...);
        void warn(char* file_name, int line_no, const char* format, ...);
        void error(char* file_name, int line_no, const char* format, ...);
        void trace(char* file_name, int line_no, const char* format, ...);
        void debug(char* file_name, int line_no, const char* format, ...);
    private:
        class LoggerGarbage {
            public:
                ~LoggerGarbage() {
                    if (NULL != KafkaLogger::instance_) {
                        delete KafkaLogger::instance_;
                    }
                }
        };

        static LoggerGarbage garb_;
};

#endif // __KAFKALOGGER_H__
