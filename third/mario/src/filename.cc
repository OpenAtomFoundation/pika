#include "filename.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "xdebug.h"

std::string CurrentFileName(const std::string& dbname)
{
    return dbname + "/CURRENT";
}

std::string NewFileName(const std::string name, const uint32_t current)
{
    char buf[256];
    snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
    return std::string(buf);
}
