#ifndef BINLOG_CONST_H_
#define BINLOG_CONST_H_

// //role
// #define PIKA_ROLE_SINGLE 0
// #define PIKA_ROLE_SLAVE 1
// #define PIKA_ROLE_MASTER 2
// #define PIKA_ROLE_DOUBLE_MASTER 3
#define PIKA_ROLE_PORT 4

#include <limits.h>
#include <stdint.h>

const int64_t kTestPoint = 500000;
const int64_t kTestNum = LLONG_MAX;
const int64_t kDataSetNum = 5;

#include <string>
std::string PikaState(int state);
std::string PikaRole(int role) ;

const char kSuffixKv = 'K';
const char kSuffixHash = 'H';
const char kSuffixZset = 'Z';
const char kSuffixSet = 'S';
const char kSuffixList = 'L';

const std::string SlotKeyPrefix = "_internal:slotkey:4migrate:";

const char* GetDBTypeString(int type);

#endif

