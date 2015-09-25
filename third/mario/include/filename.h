#ifndef __MARIO_FILENAME_H_
#define __MARIO_FILENAME_H_
#include <iostream>
#include <stdint.h>

std::string CurrentFileName(const std::string& dbname);

std::string NewFileName(const std::string name, const uint32_t current);

#endif
