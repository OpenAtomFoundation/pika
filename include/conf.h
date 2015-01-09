#ifndef __CONF_H_
#define __CONF_H_

#include "xdebug.h"

#include <errno.h>
#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdint.h>


#define WORD_SIZE 1024
#define LINE_SIZE 1024
#define CONF_LEN 1024


// define common character
#define SPACE ' '
#define COLON ':'
#define SHARP '#'



struct qf_confitem {
    char name[WORD_SIZE];
    char value[WORD_SIZE];
};

struct qf_confdata {
    int num;
    int size;
    qf_confitem *item;
};


qf_confdata* init_conf(int num);
int read_conf(const char* path, qf_confdata *confdata);
void dump_conf(qf_confdata *confdata);
int get_confint(qf_confdata *confdata, const char* name, int* value);
int get_confstr(qf_confdata *confdata, const char* name, char* value);
int free_conf(qf_confdata *confdata);



// 未实现的接口
int get_ip_port(qf_confdata *confdata, const char* name, char** ip_port);
void decode_ip_prot(const char* ip_port, char* ip, int* port);

#endif
