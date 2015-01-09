#include "base_conf.h"
#include "sys/stat.h"
#include <glog/logging.h>

BaseConf::BaseConf(const char* path)
{
    item_ = (ConfItem *)malloc(sizeof(ConfItem) * TICK_CONF_MAX_NUM);
    num_ = 0;
    size_ = TICK_CONF_MAX_NUM;
    ReadConf(path);
}

BaseConf::~BaseConf()
{
    free(item_);
}

bool BaseConf::ReadConf(const char* path)
{
    struct stat info;
    FILE* pf;

    int val = stat(path, &info);

    if (val < 0) {
        LOG(FATAL) << "Can't not found the conf file";
    }
    if (!S_ISREG(info.st_mode)) {
        LOG(FATAL) << "The conf is not a regular file";
    }

    pf = fopen(path, "r");
    if (pf == NULL) {
        LOG(FATAL) << "Open the conf file error";
    }

    // read conf items
    int item_num = 0;
    char line[TICK_WORD_SIZE];
    char ch = 0;
    int line_len = 0;
    char c_name[TICK_WORD_SIZE];
    char c_value[TICK_WORD_SIZE];
    int sep_sign = 0;
    int name_len = 0;
    int value_len = 0;

    while (fgets(line, TICK_WORD_SIZE, pf) != NULL) {
        name_len = 0;
        value_len = 0;
        sep_sign = 0;
        line_len = strlen(line);
        for (int i = 0; i < line_len; i++) {
            ch = line[i];
            switch (ch) {
                case '#': {
                    if (i == 0) {
                        continue;
                    }
                }
                case ':': {
                    sep_sign = 1;
                }
                case SPACE: {
                    continue;
                }
                case '\r': {
                    continue;
                }
                case '\n': {
                    continue;
                }
                default: {
                    if (sep_sign == 0) {
                        c_name[name_len++] = line[i];
                    } else {
                        c_value[value_len++] = line[i];
                    }
                }
            }
        }
        c_name[name_len] = '\0';
        c_value[value_len] = '\0';
        
        snprintf(item_[item_num].name, sizeof(item_[item_num].name), "%s", c_name);
        snprintf(item_[item_num].value, sizeof(item_[item_num].value), "%s", c_value);
        item_num++;
    }
    num_ = item_num;
    return 0;
}

bool BaseConf::getConfInt(const char* name, int* value)
{
    int ret;
    for (int i = 0; i < num_; i++) {
        if (strcmp(item_[i].name, name) == 0) {
            sscanf(item_[i].value, "%d", &ret);
            *value = ret;
            return 0;
        }
    }
    return 1;
}

bool BaseConf::getConfStr(const char* name, char* value)
{
    for (int i = 0; i < num_; i++) {
        if (strcmp(item_[i].name, name) == 0) {
            strcpy(value, item_[i].value);
            return 0;
        }
    }
    return 1;
}

void BaseConf::DumpConf()
{
    for (int i = 0; i < num_; i++) {
        printf("%2d %s %s\n", i + 1, item_[i].name, item_[i].value);
    }
}
