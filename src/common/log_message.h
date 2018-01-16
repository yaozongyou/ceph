#ifndef LOG_MESSAGE_H
#define LOG_MESSAGE_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#include <iostream>
#include <sstream>

#define LOG_MESSAGE(format, ...)                                                                \
    do {                                                                                        \
        FILE* ffff = fopen("/tmp/debug.log", "a");                                              \
        if (ffff == NULL) {                                                                     \
            break;                                                                              \
        }                                                                                       \
        struct timeval tv = {0};                                                                \
        struct tm tm = {0};                                                                     \
        gettimeofday(&tv, NULL);                                                                \
        localtime_r(&(tv.tv_sec), &tm);                                                         \
        fprintf(ffff, "[%d-%02d-%02d %02d:%02d:%02d.%03d][%d:%d][%s:%d] " format "\n",          \
                1900 + tm.tm_year, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, \
                static_cast<int>(tv.tv_usec / 1000),                                            \
                static_cast<int>(getpid()), static_cast<int>(syscall(SYS_gettid)),              \
                __FILE__, __LINE__,                                                             \
                ##__VA_ARGS__);                                                                 \
        fclose(ffff);                                                                           \
    } while (false)

class LogMessage {
public:
    LogMessage(const char* file, int line) {
        Init(file, line);
    }

    ~LogMessage() {
        FILE* fp = fopen("/tmp/debug.log", "a");
        if (fp == NULL) {
            return;
        }
        fprintf(fp, "%s\n", stream_.str().c_str());
        fclose(fp);
    }

    std::ostream& stream() { return stream_; }

private:
    void Init(const char* file, int line) {
        char buf[4096] = {0};
        struct timeval tv = {0};
        struct tm tm = {0};
        gettimeofday(&tv, NULL);
        localtime_r(&(tv.tv_sec), &tm);
        sprintf(buf, "[%d-%02d-%02d %02d:%02d:%02d.%03d][%d:%d][%s:%d] ",
                1900 + tm.tm_year, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
                static_cast<int>(tv.tv_usec / 1000),
                static_cast<int>(getpid()), static_cast<int>(syscall(SYS_gettid)),
                file, line);
        stream_ << buf;
    }

    std::ostringstream stream_;
};

#define LOG(INFO) LogMessage(__FILE__, __LINE__).stream()

#endif // LOG_MESSAGE_H
