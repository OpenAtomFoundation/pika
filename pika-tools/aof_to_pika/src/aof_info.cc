#include <iostream>
#include "aof_info.h"

short aof_info_level_ = AOF_LOG_INFO;

void set_info_level(int l){
    aof_info_level_ = l;
}

void info_print(int l, const std::string &content) {
    if (l > AOF_LOG_FATAL || l < AOF_LOG_DEBUG || content.empty()) return;

    if (l < aof_info_level_) return;
    if (l >= AOF_LOG_ERR)
        std::cerr << content << std::endl;
    else
        std::cout << content << std::endl;
}

