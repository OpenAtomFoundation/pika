#include "base_conf.h"
#include "sys/stat.h"
#include <glog/logging.h>
#include <algorithm>

BaseConf::BaseConf(const char* path)
{
    item_ = (ConfItem *)malloc(sizeof(ConfItem) * PIKA_CONF_MAX_NUM);
    num_ = 0;
    size_ = PIKA_CONF_MAX_NUM;
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
    char line[PIKA_WORD_SIZE];
    char ch = 0;
    int line_len = 0;
    char c_name[PIKA_WORD_SIZE];
    char c_value[PIKA_WORD_SIZE];
    int sep_sign = 0;
    int name_len = 0;
    int value_len = 0;

    while (fgets(line, PIKA_WORD_SIZE, pf) != NULL) {
        name_len = 0;
        value_len = 0;
        sep_sign = 0;
        line_len = strlen(line);
        for (int i = 0; i < line_len; i++) {
            ch = line[i];
            if (ch == '#' && sep_sign == 0) break;

            switch (ch) {
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

        if (name_len <= 0) continue;

        c_name[name_len] = '\0';
        c_value[value_len] = '\0';
        
        snprintf(item_[item_num].name, sizeof(item_[item_num].name), "%s", c_name);
        snprintf(item_[item_num].value, sizeof(item_[item_num].value), "%s", c_value);
        item_num++;
    }
    num_ = item_num;
    fclose(pf);
    return 0;
}

bool BaseConf::getConfBool(const char* name, bool* value) {
    int ret;
    for (int i = 0; i <num_; i++) {
        if (strcmp(item_[i].name, name) == 0) {
            char *p = item_[i].value;
            std::transform(p, p+strlen(p), p, ::tolower);
            if (!strcmp(p, "1") ||
                !strcmp(p, "yes")) {
                *value = true;
            } else {
                *value = false;
            }
            return true;
        }
    }
    return false;
}

bool BaseConf::getConfInt(const char* name, int* value)
{
    int ret;
    for (int i = 0; i < num_; i++) {
        if (strcmp(item_[i].name, name) == 0) {
            sscanf(item_[i].value, "%d", &ret);
            *value = ret;
            return true;
        }
    }
    return 1;
}

bool BaseConf::getConfStr(const char* name, char* value)
{
    for (int i = 0; i < num_; i++) {
        if (strcmp(item_[i].name, name) == 0) {
            strcpy(value, item_[i].value);
            return true;
        }
    }
    return false;
}

void BaseConf::DumpConf()
{
    for (int i = 0; i < num_; i++) {
        printf("%2d %s %s\n", i + 1, item_[i].name, item_[i].value);
    }
}
