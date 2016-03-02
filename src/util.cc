/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "fmacros.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <float.h>
#include <stdint.h>

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sstream>
#include <fstream>
#include <dirent.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <dirent.h>
#include <glog/logging.h>

#include "util.h"
#include "pika_conf.h"
#include "libssh2.h"

extern PikaConf* g_pikaConf;

/* Glob-style pattern matching. */
int stringmatchlen(const char *pattern, int patternLen,
        const char *string, int stringLen, int nocase)
{
    while(patternLen) {
        switch(pattern[0]) {
        case '*':
            while (pattern[1] == '*') {
                pattern++;
                patternLen--;
            }
            if (patternLen == 1)
                return 1; /* match */
            while(stringLen) {
                if (stringmatchlen(pattern+1, patternLen-1,
                            string, stringLen, nocase))
                    return 1; /* match */
                string++;
                stringLen--;
            }
            return 0; /* no match */
            break;
        case '?':
            if (stringLen == 0)
                return 0; /* no match */
            string++;
            stringLen--;
            break;
        case '[':
        {
            int nott;
            int match;

            pattern++;
            patternLen--;
            nott = pattern[0] == '^';
            if (nott) {
                pattern++;
                patternLen--;
            }
            match = 0;
            while(1) {
                if (pattern[0] == '\\') {
                    pattern++;
                    patternLen--;
                    if (pattern[0] == string[0])
                        match = 1;
                } else if (pattern[0] == ']') {
                    break;
                } else if (patternLen == 0) {
                    pattern--;
                    patternLen++;
                    break;
                } else if (pattern[1] == '-' && patternLen >= 3) {
                    int start = pattern[0];
                    int end = pattern[2];
                    int c = string[0];
                    if (start > end) {
                        int t = start;
                        start = end;
                        end = t;
                    }
                    if (nocase) {
                        start = tolower(start);
                        end = tolower(end);
                        c = tolower(c);
                    }
                    pattern += 2;
                    patternLen -= 2;
                    if (c >= start && c <= end)
                        match = 1;
                } else {
                    if (!nocase) {
                        if (pattern[0] == string[0])
                            match = 1;
                    } else {
                        if (tolower((int)pattern[0]) == tolower((int)string[0]))
                            match = 1;
                    }
                }
                pattern++;
                patternLen--;
            }
            if (nott)
                match = !match;
            if (!match)
                return 0; /* no match */
            string++;
            stringLen--;
            break;
        }
        case '\\':
            if (patternLen >= 2) {
                pattern++;
                patternLen--;
            }
            /* fall through */
        default:
            if (!nocase) {
                if (pattern[0] != string[0])
                    return 0; /* no match */
            } else {
                if (tolower((int)pattern[0]) != tolower((int)string[0]))
                    return 0; /* no match */
            }
            string++;
            stringLen--;
            break;
        }
        pattern++;
        patternLen--;
        if (stringLen == 0) {
            while(*pattern == '*') {
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    if (patternLen == 0 && stringLen == 0)
        return 1;
    return 0;
}

int stringmatch(const char *pattern, const char *string, int nocase) {
    return stringmatchlen(pattern,strlen(pattern),string,strlen(string),nocase);
}

/* Convert a string representing an amount of memory into the number of
 * bytes, so for instance memtoll("1Gi") will return 1073741824 that is
 * (1024*1024*1024).
 *
 * On parsing error, if *err is not NULL, it's set to 1, otherwise it's
 * set to 0 */
long long memtoll(const char *p, int *err) {
    const char *u;
    char buf[128];
    long mul; /* unit multiplier */
    long long val;
    unsigned int digits;

    if (err) *err = 0;
    /* Search the first non digit character. */
    u = p;
    if (*u == '-') u++;
    while(*u && isdigit(*u)) u++;
    if (*u == '\0' || !strcasecmp(u,"b")) {
        mul = 1;
    } else if (!strcasecmp(u,"k")) {
        mul = 1000;
    } else if (!strcasecmp(u,"kb")) {
        mul = 1024;
    } else if (!strcasecmp(u,"m")) {
        mul = 1000*1000;
    } else if (!strcasecmp(u,"mb")) {
        mul = 1024*1024;
    } else if (!strcasecmp(u,"g")) {
        mul = 1000L*1000*1000;
    } else if (!strcasecmp(u,"gb")) {
        mul = 1024L*1024*1024;
    } else {
        if (err) *err = 1;
        mul = 1;
    }
    digits = u-p;
    if (digits >= sizeof(buf)) {
        if (err) *err = 1;
        return LLONG_MAX;
    }
    memcpy(buf,p,digits);
    buf[digits] = '\0';
    val = strtoll(buf,NULL,10);
    return val*mul;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
uint32_t digits10(uint64_t v) {
    if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < 1000000000000UL) {
        if (v < 100000000UL) {
            if (v < 1000000) {
                if (v < 10000) return 4;
                return 5 + (v >= 100000);
            }
            return 7 + (v >= 10000000UL);
        }
        if (v < 10000000000UL) {
            return 9 + (v >= 1000000000UL);
        }
        return 11 + (v >= 100000000000UL);
    }
    return 12 + digits10(v / 1000000000000UL);
}

/* Convert a long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned.
 *
 * Based on the following article (that apparently does not provide a
 * novel approach but only publicizes an already used technique):
 *
 * https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920
 *
 * Modified in order to handle signed integers since the original code was
 * designed for unsigned integers. */
int ll2string(char* dst, size_t dstlen, long long svalue) {
    static const char digits[201] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";
    int negative;
    unsigned long long value;

    /* The main loop works with 64bit unsigned integers for simplicity, so
     * we convert the number here and remember if it is negative. */
    if (svalue < 0) {
        if (svalue != LLONG_MIN) {
            value = -svalue;
        } else {
            value = ((unsigned long long) LLONG_MAX)+1;
        }
        negative = 1;
    } else {
        value = svalue;
        negative = 0;
    }

    /* Check length. */
    uint32_t const length = digits10(value)+negative;
    if (length >= dstlen) return 0;

    /* Null term. */
    uint32_t next = length;
    dst[next] = '\0';
    next--;
    while (value >= 100) {
        int const i = (value % 100) * 2;
        value /= 100;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
        next -= 2;
    }

    /* Handle last 1-2 digits. */
    if (value < 10) {
        dst[next] = '0' + (uint32_t) value;
    } else {
        int i = (uint32_t) value * 2;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
    }

    /* Add sign. */
    if (negative) dst[0] = '-';
    return length;
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(const char *s, size_t slen, long long *value) {
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    unsigned long long v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }
    
    while(plen < slen && p[0] == '0') {
        p++; plen++;
    }

    if (plen == slen) {
        if (value != NULL) *value = 0;
        return 1;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

/* Convert a string into a long. Returns 1 if the string could be parsed into a
 * (non-overflowing) long, 0 otherwise. The value will be set to the parsed
 * value when appropriate. */
int string2l(const char *s, size_t slen, long *lval) {
    long long llval;

    if (!string2ll(s,slen,&llval))
        return 0;

    if (llval < LONG_MIN || llval > LONG_MAX)
        return 0;

    *lval = (long)llval;
    return 1;
}

/* Convert a double to a string representation. Returns the number of bytes
 * required. The representation should always be parsable by strtod(3). */
int d2string(char *buf, size_t len, double value) {
    if (isnan(value)) {
        len = snprintf(buf,len,"nan");
    } else if (isinf(value)) {
        if (value < 0)
            len = snprintf(buf,len,"-inf");
        else
            len = snprintf(buf,len,"inf");
    } else if (value == 0) {
        /* See: http://en.wikipedia.org/wiki/Signed_zero, "Comparisons". */
        if (1.0/value < 0)
            len = snprintf(buf,len,"-0");
        else
            len = snprintf(buf,len,"0");
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (value > min && value < max && value == ((double)((long long)value)))
            len = ll2string(buf,len,(long long)value);
        else
#endif
            len = snprintf(buf,len,"%.17g",value);
    }

    return len;
}

int string2d(const char *s, size_t slen, double *dval) {
    char *pEnd;
    double d = strtod(s, &pEnd);
    if (pEnd != s + slen)
        return 0;

    if (dval != NULL) *dval = d;
    return 1;
}

/* Generate the Redis "Run ID", a SHA1-sized random number that identifies a
 * given execution of Redis, so that if you are talking with an instance
 * having run_id == A, and you reconnect and it has run_id == B, you can be
 * sure that it is either a different instance or it was restarted. */
void getRandomHexChars(char *p, unsigned int len) {
    FILE *fp = fopen("/dev/urandom","r");
    char charset[] = "0123456789abcdef";
    unsigned int j;

    if (fp == NULL || fread(p,len,1,fp) == 0) {
        /* If we can't read from /dev/urandom, do some reasonable effort
         * in order to create some entropy, since this function is used to
         * generate run_id and cluster instance IDs */
        char *x = p;
        unsigned int l = len;
        struct timeval tv;
        pid_t pid = getpid();

        /* Use time and PID to fill the initial array. */
        gettimeofday(&tv,NULL);
        if (l >= sizeof(tv.tv_usec)) {
            memcpy(x,&tv.tv_usec,sizeof(tv.tv_usec));
            l -= sizeof(tv.tv_usec);
            x += sizeof(tv.tv_usec);
        }
        if (l >= sizeof(tv.tv_sec)) {
            memcpy(x,&tv.tv_sec,sizeof(tv.tv_sec));
            l -= sizeof(tv.tv_sec);
            x += sizeof(tv.tv_sec);
        }
        if (l >= sizeof(pid)) {
            memcpy(x,&pid,sizeof(pid));
            l -= sizeof(pid);
            x += sizeof(pid);
        }
        /* Finally xor it with rand() output, that was already seeded with
         * time() at startup. */
        for (j = 0; j < len; j++)
            p[j] ^= rand();
    }
    /* Turn it into hex digits taking just 4 bits out of 8 for every byte. */
    for (j = 0; j < len; j++)
        p[j] = charset[p[j] & 0x0F];
    if (fp) fclose(fp);
}

/* Given the filename, return the absolute path as an SDS string, or NULL
 * if it fails for some reason. Note that "filename" may be an absolute path
 * already, this will be detected and handled correctly.
 *
 * The function does not try to normalize everything, but only the obvious
 * case of one or more "../" appearning at the start of "filename"
 * relative path. */
sds getAbsolutePath(char *filename) {
    char cwd[1024];
    sds abspath;
    sds relpath = sdsnew(filename);

    relpath = sdstrim(relpath," \r\n\t");
    if (relpath[0] == '/') return relpath; /* Path is already absolute. */

    /* If path is relative, join cwd and relative path. */
    if (getcwd(cwd,sizeof(cwd)) == NULL) {
        sdsfree(relpath);
        return NULL;
    }
    abspath = sdsnew(cwd);
    if (sdslen(abspath) && abspath[sdslen(abspath)-1] != '/')
        abspath = sdscat(abspath,"/");

    /* At this point we have the current path always ending with "/", and
     * the trimmed relative path. Try to normalize the obvious case of
     * trailing ../ elements at the start of the path.
     *
     * For every "../" we find in the filename, we remove it and also remove
     * the last element of the cwd, unless the current cwd is "/". */
    while (sdslen(relpath) >= 3 &&
           relpath[0] == '.' && relpath[1] == '.' && relpath[2] == '/')
    {
        sdsrange(relpath,3,-1);
        if (sdslen(abspath) > 1) {
            char *p = abspath + sdslen(abspath)-2;
            int trimlen = 1;

            while(*p != '/') {
                p--;
                trimlen++;
            }
            sdsrange(abspath,0,-(trimlen+1));
        }
    }

    /* Finally glue the two parts together. */
    abspath = sdscatsds(abspath,relpath);
    sdsfree(relpath);
    return abspath;
}

/* Return true if the specified path is just a file basename without any
 * relative or absolute path. This function just checks that no / or \
 * character exists inside the specified path, that's enough in the
 * environments where Redis runs. */
int pathIsBaseName(char *path) {
    return strchr(path,'/') == NULL && strchr(path,'\\') == NULL;
}

int delete_dir(const char* dirname)
{
    char chBuf[256];
    DIR * dir = NULL;
    struct dirent *ptr;
    int ret = 0;
    dir = opendir(dirname);
    if (NULL == dir) {
        return -1;
    }
    while((ptr = readdir(dir)) != NULL) {
        ret = strcmp(ptr->d_name, ".");
        if (0 == ret) {
            continue;
        }
        ret = strcmp(ptr->d_name, "..");
        if (0 == ret) {
            continue;
        }
        snprintf(chBuf, 256, "%s/%s", dirname, ptr->d_name);
        ret = is_dir(chBuf);
        if (0 == ret) {
            //is dir
            ret = delete_dir(chBuf);
            if (0 != ret) {
                return -1;
            }
        }
        else if (1 == ret) {
            //is file
            ret = remove(chBuf);
            if(0 != ret) {
                return -1;
            }
        }
    }
    (void)closedir(dir);
    ret = remove(dirname);
    if (0 != ret) {
        return -1;
    }
    return 0;
}

void* remove_dir(void *arg) {
    assert(arg != NULL);
    delete_dir((char*)arg);
    free(arg);
    return NULL;
}

int remove_files(const char* path, const char* pattern) {
    DIR *dir = NULL;
    dir = opendir(path);
    if (dir == NULL) {
        return errno;
    }
    char dir_path[100], whole_file_path[100];
    strcpy(dir_path, path);
    if (*(dir_path + strlen(dir_path) -1) == '/') {
        *(dir_path + strlen(dir_path) -1) = '\0';
    }
    struct dirent *dirent_ptr = NULL;
    while ((dirent_ptr = readdir(dir)) != NULL) {
        if (!strcmp(dirent_ptr->d_name, ".") || !strcmp(dirent_ptr->d_name, "..")) {
            continue;
        }
        if (stringmatch(pattern, dirent_ptr->d_name, false)) {
            snprintf(whole_file_path, sizeof(whole_file_path), "%s/%s", dir_path, dirent_ptr->d_name);
            remove(whole_file_path);
        }
    }
    return 0;
}

int do_mkdir(const char *path, mode_t mode) {
  struct stat st;
  int status = 0;

  if (stat(path, &st) != 0) {
    /* Directory does not exist. EEXIST for race
     * condition */
    if (mkdir(path, mode) != 0 && errno != EEXIST)
      status = -1;
  } else if (!S_ISDIR(st.st_mode)) {
    errno = ENOTDIR;
    status = -1;
  }
  if (chmod(path, mode) != 0) { //modify the priority
    status = -1;
  }
  return (status);
}

int mkpath(const char *path, mode_t mode) {
  char           *pp;
  char           *sp;
  int             status;
  char           *copypath = strdup(path);

  status = 0;
  pp = copypath;
  while (status == 0 && (sp = strchr(pp, '/')) != 0) {
    if (sp != pp) {
      /* Neither root nor double slash in path */
      *sp = '\0';
      status = do_mkdir(copypath, mode);
      *sp = '/';
    }
    pp = sp + 1;
  }
  if (status == 0)
    status = do_mkdir(path, mode);
  free(copypath);
  return (status);
}

int copy_file(const char *src_file, const char *dst_file) {
    FILE* infile = NULL, *outfile = NULL;
    infile = fopen(src_file, "rb");
    if (infile == NULL) {
        return -1;
    }
    outfile = fopen(dst_file, "wb");
    if (outfile == NULL) {
        return -2;
    }
#define BUFFER_SIZE 4096
    char buf[BUFFER_SIZE];
    int32_t nread = 0;
    int32_t nwrite = 0;
    while (nread = fread(buf, 1, BUFFER_SIZE, infile)) {
        nwrite = fwrite(buf, 1, nread, outfile);
        if (nwrite < nread) {
            return -3;
        }
        if (nread < BUFFER_SIZE) {
            break;
        }
    }
    if (!feof(infile)) {
        return -4;
    }
    fclose(infile);
    fclose(outfile);
    return 0;
}

int is_dir(const char* filename) {
    struct stat buf;
    int ret = stat(filename,&buf);
    if (0 == ret) {
        if (buf.st_mode & S_IFDIR) {
            //folder
            return 0;
        } else {
            //file
            return 1;
        }
    }
    return -1;
}

int copy_dir(const char *src_dir_path, const char *dst_dir_path) {
    int ret;
    DIR *src_dir = NULL, *dst_dir = NULL;
    src_dir = opendir(src_dir_path);
    if (src_dir == NULL) {
        return -1;
    }
    dst_dir = opendir(dst_dir_path);
    if (dst_dir == NULL) {
        mkdir(dst_dir_path, 0755);
        dst_dir = opendir(dst_dir_path);
    }
    struct dirent *dirent_ptr = NULL;
    while ((dirent_ptr = readdir(src_dir)) != NULL) {
        char src_whole_path[100];
        char dst_whole_path[100];
        if (!strcmp(dirent_ptr->d_name, ".") || !strcmp(dirent_ptr->d_name, "..")) {
            continue;
        }
        snprintf(src_whole_path, sizeof(src_whole_path), "%s/%s", src_dir_path, dirent_ptr->d_name);
        snprintf(dst_whole_path, sizeof(dst_whole_path), "%s/%s", dst_dir_path, dirent_ptr->d_name);
        if (is_dir(src_whole_path) == 0) {
            if (ret = copy_dir(src_whole_path, dst_whole_path)) {
                return ret;
            }
        } else if (ret = copy_file(src_whole_path, dst_whole_path)) {
            return ret;
        }
    }
    closedir(src_dir);
    closedir(dst_dir);
    return 0;
}

void scp_write_file_clean(int sockfd, LIBSSH2_SESSION *session, LIBSSH2_CHANNEL* channel, FILE* local_file) {
    if (session) {
        libssh2_session_disconnect(session, "");
        libssh2_session_free(session);
        libssh2_exit();
    }
    if (sockfd != -1) {
        close(sockfd);
    }
    if (local_file) {
        fclose(local_file);
    }
}

int scp_write_file(const char* local_file_path, const char* dst_file_path, const char* dst_host_addr, const char* username, const char* password) {
#define SCP_BUFFER_LEN 4096
    unsigned long host_addr = inet_addr(dst_host_addr);
    int sockfd = -1;
    LIBSSH2_SESSION *session = NULL;
    LIBSSH2_CHANNEL *channel = NULL;
    int rc;
    FILE* local_file = NULL;
    struct stat file_info;
    struct sockaddr_in sin;
    size_t nread;
    char mem[SCP_BUFFER_LEN];
    char *ptr;

    rc = libssh2_init(0);
    if (rc) {
        //fprintf(stderr, "libssh2 init error (%d)\n", rc);
        LOG(ERROR) << "libssh2 init error (" << rc << ")";
        return -1;
    }
    local_file = fopen(local_file_path, "rb");
    if (!local_file) {
        //fprintf(stderr, "local file %s open error", local_file_path);
        LOG(ERROR) << "local file " << local_file_path << " open error";
        return -2;
    }
    stat(local_file_path, &file_info);
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        //fprintf(stderr, "failed to create socket\n");
        LOG(ERROR) << "failed to create socket";
        scp_write_file_clean(sockfd, session, channel, local_file);
        return -3;
    }
    bzero(&sin, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(22);
    sin.sin_addr.s_addr = host_addr;
    if (connect(sockfd, (struct sockaddr*)(&sin), sizeof(struct sockaddr_in))) {
        //fprintf(stderr, "failed to connect\n");
        LOG(ERROR) << "failed to connect";
        scp_write_file_clean(sockfd, session, channel, local_file);
        return -4;
    }
    session = libssh2_session_init();
    if (session == NULL) {
        //fprintf(stderr, "libssh2 session create failed\n");
        LOG(ERROR) << "libssh2 session create failed";
        scp_write_file_clean(sockfd, session, channel, local_file);
        return -5;
    }
    rc = libssh2_session_handshake(session, sockfd);
    if (rc) {
        //fprintf(stderr, "failed to establish the ssh session (%d)\n", rc);
        LOG(ERROR) << "failed to establish the ssh session (" << rc << ")";
        scp_write_file_clean(sockfd, session, channel, local_file);
        return -6;
    }
    if (libssh2_userauth_password(session, username, password)) {
        //fprintf(stderr, "authentication by password failed\n");
        LOG(ERROR) << "authentication by password failed";
        scp_write_file_clean(sockfd, session, channel, local_file);
        return -7;
    }
    channel = libssh2_scp_send(session, dst_file_path, file_info.st_mode & 0777, (unsigned long)file_info.st_size);
    if (channel == NULL) {
        char *errmsg;
        int errlen;
        int err = libssh2_session_last_error(session, &errmsg, &errlen, 0);
        //fprintf(stderr, "unable to open a session (%d) %s\n", err, errmsg);
        LOG(ERROR) << "unable to open a session (" << err << ") " << errmsg;
        scp_write_file_clean(sockfd, session, channel, local_file);
        return -8;
    }
    //fprintf(stderr, "local file %s transfer started\n", local_file_path);
    LOG(WARNING) << "local file " << local_file_path << "transfer started";
    int32_t cur_speed_bytes = (g_pikaConf->db_sync_speed())*1024*1024;
    int32_t now_bytes = 0;
    struct timeval last_time, cur_time;
    gettimeofday(&last_time, NULL);
    do {
        nread = fread(mem, 1, sizeof(mem), local_file);
        if (nread <= 0) {
            break;
        }
        now_bytes += nread;
        ptr = mem;
        do {
            rc = libssh2_channel_write(channel, ptr, nread);
            if (rc < 0) {
                fprintf(stderr, "ERROR (%d)\n", rc);
                //LOG(ERROR) << "ERROR (" << rc << ")";
                break;
            } else {
                ptr += rc;
                nread -= rc;
            }
        } while (nread);
        if (now_bytes <= cur_speed_bytes) {
            continue;
        }
        gettimeofday(&cur_time, NULL);
        if (cur_time.tv_sec == last_time.tv_sec) {
            usleep(1000000L-cur_time.tv_usec+last_time.tv_usec);
            last_time.tv_sec += 1;
        } else {
            last_time.tv_sec = cur_time.tv_sec;
            last_time.tv_usec = cur_time.tv_usec;
        }
        cur_speed_bytes = (g_pikaConf->db_sync_speed())*1024*1024;
        now_bytes = 0;
    } while (1);

    //fprintf(stderr, "Sending EOF\n");
    libssh2_channel_send_eof(channel);
    //fprintf(stderr, "Waiting for EOF\n");
    libssh2_channel_wait_eof(channel);
    //fprintf(stderr, "Wait for channel to close\n");
    libssh2_channel_wait_closed(channel);
    libssh2_channel_free(channel);
    channel = NULL;
    if (!feof(local_file)) {
        scp_write_file_clean(sockfd, session, channel, local_file);
        //fprintf(stderr, "local file %s read error\n", local_file_path);
        LOG(ERROR) << "local file " << local_file_path << "read error";
        return -9;
    }
    scp_write_file_clean(sockfd, session, channel, local_file);
    //fprintf(stderr, "local file %s transfer finished\n", local_file_path);
    LOG(WARNING) << "local file " << local_file_path << "transfer finished";
    return 0;
}

int scp_copy_dir(const char* local_dir_path, const char* remote_dir_path, const char* remote_host, const char* username, const char* password) {
    int ret;
    struct dirent* dirent_ptr = NULL;
    struct stat file_info;
    DIR* local_dir = opendir(local_dir_path);
    if (local_dir == NULL) {
        return -1;
    }
    while ((dirent_ptr = readdir(local_dir)) != NULL) {
        char local_file_whole_path[100];
        char remote_file_whole_path[100];
        if (!strcmp(dirent_ptr->d_name, ".") || !strcmp(dirent_ptr->d_name, "..")) {
            continue;
        }

        char dir_path[100];

        strcpy(dir_path, local_dir_path);
        if (dir_path[strlen(dir_path)-1] == '/') {
            dir_path[strlen(dir_path)-1] = '\0';
        }
        snprintf(local_file_whole_path, sizeof(local_file_whole_path), "%s/%s", dir_path, dirent_ptr->d_name);
        strcpy(dir_path, remote_dir_path);
        if (dir_path[strlen(dir_path)-1] == '/') {
            dir_path[strlen(dir_path)-1] = '\0';
        }
        snprintf(remote_file_whole_path, sizeof(remote_file_whole_path), "%s/%s", dir_path, dirent_ptr->d_name);
        if (stat(local_file_whole_path, &file_info) != 0) {
            closedir(local_dir);
            return -2;
        }
        if (file_info.st_mode & S_IFDIR) {
            ret = scp_copy_dir(local_file_whole_path, remote_file_whole_path, remote_host, username, password);
        } else {
            ret = scp_write_file(local_file_whole_path, remote_file_whole_path, remote_host, username, password);
        }
        if (ret != 0) {
            closedir(local_dir);
            return -3;
        }
    }
    closedir(local_dir);
    return 0;
}

int rsync_copy_file(const char* local_file_path, const char* remote_file_path, const char* remote_host, const int dest_rsync_port) {
    std::string rsync_cmd = "rsync -avP --bwlimit=";
    int32_t bwlimit_kb = g_pikaConf->db_sync_speed() * 1024;
    char buf[20];
    snprintf(buf, sizeof(buf), "%d", bwlimit_kb);
    rsync_cmd.append(buf);
    if (dest_rsync_port != 873) {
        snprintf(buf, sizeof(buf), "%d", dest_rsync_port);
        rsync_cmd.append(" --port=");
        rsync_cmd.append(buf);
    }
    rsync_cmd.append(" ");
    rsync_cmd.append(local_file_path, strlen(local_file_path));
    rsync_cmd.append(" ");
    rsync_cmd.append(remote_host);
    rsync_cmd.append("::");
    rsync_cmd.append(remote_file_path);

    LOG(WARNING) << rsync_cmd << " start...";
    int ret = system(rsync_cmd.c_str());
//    if (ret == -1 || ret == 127) {
    if (ret == 0 || (WIFEXITED(ret) && !WEXITSTATUS(ret))) {
        LOG(WARNING) << rsync_cmd << " finished...";
        return 0;
    } else {
        LOG(WARNING) << "rsync " << local_file_path << " failed" << std::endl;
        return -1;
    }
}

int rsync_copy_dir(const char* local_dir_path, const char* remote_dir_path, const char* remote_host, const int dest_rsync_port) {
    int ret;
    struct dirent* dirent_ptr = NULL;
    struct stat file_info;
    DIR* local_dir = opendir(local_dir_path);
    if (local_dir == NULL) {
        LOG(WARNING) << "open local dir path failed";
        return -1;
    }
    while ((dirent_ptr = readdir(local_dir)) != NULL) {
        char local_file_whole_path[100];
        char remote_file_whole_path[100];
        if (!strcmp(dirent_ptr->d_name, ".") || !strcmp(dirent_ptr->d_name, "..")) {
            continue;
        }

        char dir_path[100];

        strcpy(dir_path, local_dir_path);
        if (dir_path[strlen(dir_path)-1] == '/') {
            dir_path[strlen(dir_path)-1] = '\0';
        }
        snprintf(local_file_whole_path, sizeof(local_file_whole_path), "%s/%s", dir_path, dirent_ptr->d_name);
        strcpy(dir_path, remote_dir_path);
        if (dir_path[strlen(dir_path)-1] == '/') {
            dir_path[strlen(dir_path)-1] = '\0';
        }
        snprintf(remote_file_whole_path, sizeof(remote_file_whole_path), "%s/%s", dir_path, dirent_ptr->d_name);
        if (stat(local_file_whole_path, &file_info) != 0) {
            closedir(local_dir);
            return -2;
        }
        if (file_info.st_mode & S_IFDIR) {
            ret = rsync_copy_dir(local_file_whole_path, remote_file_whole_path, remote_host, dest_rsync_port);
        } else {
            ret = rsync_copy_file(local_file_whole_path, remote_file_whole_path, remote_host, dest_rsync_port);
        }
        if (ret != 0) {
            closedir(local_dir);
            return -3;
        }
    }
    closedir(local_dir);
    return 0;
}

int start_rsync(const std::string& path, const int rsync_port) {
    std::string path_up = path.back() == '/'? path.substr(0, path.size()-1) : path;
    if (path_up == ".") {
        char buf[100];
        getcwd(buf, sizeof(buf));
        path_up = buf;
    }
    if (!path_up.empty()) {
        size_t last_slash_pos = path_up.find_last_of("/");
        path_up = path_up.substr(0, last_slash_pos);
    }

    std::string rsync_path;
    char pid_str[10];
    std::string rsync_conf_path;
    std::string rsync_pid_file_path;

    snprintf(pid_str, sizeof(pid_str), "%d", getpid());
    rsync_path = path_up + "/rsync";
    mkpath(rsync_path.c_str(), 0755);
    rsync_conf_path = rsync_path + "/pika_rsync.conf";
    rsync_pid_file_path = rsync_path + "/pika_rsync_" + pid_str + ".pid";
    if (access(rsync_pid_file_path.c_str(), F_OK) == 0) {
        LOG(WARNING) << "rsync service already started";
        return 0;
    }
    std::ofstream rsync_conf_file(rsync_conf_path.c_str());
    if (!rsync_conf_file) {
        LOG(ERROR) << "open new rsync_conf_file failed";
        return -1;
    }
//    if (rsync_port != 873) {
//        rsync_conf_file << "port = " << rsync_port << std::endl;
//    }
    rsync_conf_file << "uid = root" << std::endl;
    rsync_conf_file << "gid = root" << std::endl;
    rsync_conf_file << "use chroot = no" << std::endl;
    rsync_conf_file << "max connections = 10" << std::endl;
    rsync_conf_file << "pid file = " << rsync_pid_file_path << std::endl;
    //rsync_conf_file << "lock file = " << rsync_path << "/pika_rsync_" << pid_str << ".lock" << std::endl;
    //rsync_conf_file << "log file = " << rsync_path << "/pika_rsync_" << pid_str << ".log" << std::endl;
    rsync_conf_file << "list = no" << std::endl;
    rsync_conf_file << "strict modes = no" << std::endl;
    rsync_conf_file << "[pika_slave_db_sync_path_" << pid_str << "]" << std::endl;
    rsync_conf_file << "path = " << path << std::endl;
    rsync_conf_file << "read only = no" << std::endl;
    rsync_conf_file.close();

    std::string rsync_start_cmd = "rsync --daemon --config=";
    rsync_start_cmd += rsync_conf_path;
    if (rsync_port != 873) {
        char buf[10];
        rsync_start_cmd.append(" --port=");
        snprintf(buf, sizeof(buf), "%d", rsync_port);
        rsync_start_cmd.append(buf);
    }
    system(rsync_start_cmd.c_str());
    LOG(WARNING) << rsync_start_cmd;
    if (access(rsync_pid_file_path.c_str(), F_OK) == 0) {
        LOG(ERROR) << "start rsync service sucess";
        return -1;
    }
    usleep(1000);
    if (access(rsync_pid_file_path.c_str(), F_OK) == 0) {
        LOG(ERROR) << "start rsync service sucess";
        return -1;
    }
    LOG(WARNING) << "start rsync service failed";
    return 0;
}

int stop_rsync(const std::string& path) {
    std::string path_up = path.back() == '/'? path.substr(0, path.size()-1) : path;
    if (path_up == ".") {
        char buf[100];
        getcwd(buf, sizeof(buf));
        path_up = buf;
    }
    if (!path_up.empty()) {
        size_t last_slash_pos = path_up.find_last_of("/");
        path_up = path_up.substr(0, last_slash_pos);
    }

    std::string rsync_path;
    char pid_str[10];
    std::string rsync_pid_file_path;

    snprintf(pid_str, sizeof(pid_str), "%d", getpid());
    rsync_path = path_up + "/rsync";
    rsync_pid_file_path = rsync_path + "/pika_rsync_" + pid_str + ".pid";

    if (access(rsync_pid_file_path.c_str(), F_OK) == -1) {
        LOG(WARNING) << "rsync service has been stoped";
        return 0;
    }
    std::string rsync_stop_cmd = "kill `cat ";
    rsync_stop_cmd.append(rsync_pid_file_path.c_str());
    rsync_stop_cmd.append("`");
    system(rsync_stop_cmd.c_str());
    sleep(1);
    if (access(rsync_pid_file_path.c_str(), F_OK) == -1) {
        LOG(WARNING) << "rsync service stoped success";
        return 0;
    }
    sleep(2);
    if (access(rsync_pid_file_path.c_str(), F_OK) == -1) {
        LOG(WARNING) << "rsync service stoped success";
        return 0;
    }
    LOG(ERROR) << "rsync service stoped failed";
    return -1;
}

int64_t ustime() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int64_t ust;
    ust = (tv.tv_sec)*1000000 + tv.tv_usec;
    return ust;
}

std::vector<std::string>& PStringSplit(const std::string &s, 
        char delim, std::vector<std::string> &elems) { 
    elems.clear();
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (!item.empty())
            elems.push_back(item);
    }
    return elems;
}

std::string PStringConcat(const std::vector<std::string> &elems, char delim) {
    std::string result;
    std::vector<std::string>::const_iterator it = elems.begin();
    while (it != elems.end()) {
       result.append(*it);
       result.append(1, delim);
       ++it;
    }
    if (!result.empty()) {
        result.resize(result.size() - 1);
    }
    return result;
}

#ifdef UTIL_TEST_MAIN

#include <assert.h>

void test_string2ll(void) {
    char buf[32];
    long long v;

    /* May not start with +. */
    strcpy(buf,"+1");
    assert(string2ll(buf,strlen(buf),&v) == 0);

    /* Leading space. */
    strcpy(buf," 1");
    assert(string2ll(buf,strlen(buf),&v) == 0);

    /* Trailing space. */
    strcpy(buf,"1 ");
    assert(string2ll(buf,strlen(buf),&v) == 0);

    /* May not start with 0. */
    strcpy(buf,"01");
    assert(string2ll(buf,strlen(buf),&v) == 0);

    strcpy(buf,"-1");
    assert(string2ll(buf,strlen(buf),&v) == 1);
    assert(v == -1);

    strcpy(buf,"0");
    assert(string2ll(buf,strlen(buf),&v) == 1);
    assert(v == 0);

    strcpy(buf,"1");
    assert(string2ll(buf,strlen(buf),&v) == 1);
    assert(v == 1);

    strcpy(buf,"99");
    assert(string2ll(buf,strlen(buf),&v) == 1);
    assert(v == 99);

    strcpy(buf,"-99");
    assert(string2ll(buf,strlen(buf),&v) == 1);
    assert(v == -99);

    strcpy(buf,"-9223372036854775808");
    assert(string2ll(buf,strlen(buf),&v) == 1);
    assert(v == LLONG_MIN);

    strcpy(buf,"-9223372036854775809"); /* overflow */
    assert(string2ll(buf,strlen(buf),&v) == 0);

    strcpy(buf,"9223372036854775807");
    assert(string2ll(buf,strlen(buf),&v) == 1);
    assert(v == LLONG_MAX);

    strcpy(buf,"9223372036854775808"); /* overflow */
    assert(string2ll(buf,strlen(buf),&v) == 0);
}

void test_string2l(void) {
    char buf[32];
    long v;

    /* May not start with +. */
    strcpy(buf,"+1");
    assert(string2l(buf,strlen(buf),&v) == 0);

    /* May not start with 0. */
    strcpy(buf,"01");
    assert(string2l(buf,strlen(buf),&v) == 0);

    strcpy(buf,"-1");
    assert(string2l(buf,strlen(buf),&v) == 1);
    assert(v == -1);

    strcpy(buf,"0");
    assert(string2l(buf,strlen(buf),&v) == 1);
    assert(v == 0);

    strcpy(buf,"1");
    assert(string2l(buf,strlen(buf),&v) == 1);
    assert(v == 1);

    strcpy(buf,"99");
    assert(string2l(buf,strlen(buf),&v) == 1);
    assert(v == 99);

    strcpy(buf,"-99");
    assert(string2l(buf,strlen(buf),&v) == 1);
    assert(v == -99);

#if LONG_MAX != LLONG_MAX
    strcpy(buf,"-2147483648");
    assert(string2l(buf,strlen(buf),&v) == 1);
    assert(v == LONG_MIN);

    strcpy(buf,"-2147483649"); /* overflow */
    assert(string2l(buf,strlen(buf),&v) == 0);

    strcpy(buf,"2147483647");
    assert(string2l(buf,strlen(buf),&v) == 1);
    assert(v == LONG_MAX);

    strcpy(buf,"2147483648"); /* overflow */
    assert(string2l(buf,strlen(buf),&v) == 0);
#endif
}
int main(int argc, char **argv) {
    test_string2ll();
    test_string2l();
    return 0;
}

#endif

