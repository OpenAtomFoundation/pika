#include <string.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <iostream>

#include <hiredis.h>
#include <unistd.h>

#define VERSION "1.0.0"

enum Status {
    Begin = 0,
    Wait,   // skip charator on src
    Copy,   // upon this status copy from src to des
    Escape,  // read backslash
    Error
};
enum Condition {
    Quotation = 0,
    Backslash,
    Others
};

// Parse reply content with status machine
static bool reply_parse(char* src, std::size_t len, std::vector<const char *> &argv, std::vector<size_t> &argv_len) {
    if (src == NULL || len == 0) return -1;

    char trans[5][3];
    memset(trans, Error, sizeof(trans));
    trans[Begin][Quotation] = Copy;
    trans[Begin][Others] = Begin;
    trans[Wait][Quotation] = Copy;
    trans[Wait][Others] = Wait;
    trans[Copy][Quotation] = Wait;
    trans[Copy][Others] = Copy;
    trans[Copy][Backslash] = Escape;
    trans[Escape][Quotation] = Copy;
    trans[Escape][Backslash] = Copy;
    trans[Escape][Others] = Copy;

    int i = 0, cur_status = Begin, input = Others;
    std::string des;
    while (i < (int)len) {
        if (cur_status == Copy || cur_status == Escape)  des.push_back(src[i]);
        switch (src[i])
        {
            case '"':
                input = Quotation;
                break;
            case '\\':
                input = Backslash;
                break;
            default:
                input = Others;
        }
        cur_status = trans[cur_status][input];
        
        if (cur_status == Error) break;

        if (cur_status == Wait && !des.empty()) {
            char *buf = (char*) malloc (des.size());
            if (buf == NULL) {
                std::cerr << "Malloc failed!" << std::endl;
                exit(0);
            }
            strncpy(buf, des.c_str(), des.size() - 1); //remove last quotation
            buf[des.size() - 1] = '\0';
            argv.push_back(buf);
            argv_len.push_back(des.size() - 1);
            //std::cout << buf << std::endl;
            des.clear();
        }
        
        i++;
    }
    if (cur_status != Wait){
        std::cerr << "Error reply content" << std::endl;
        return false;
    }
    return true;
}

static bool remote_conn(redisContext **conn, const std::string &hostname, const std::string &port, const std::string &auth="") {
    if (hostname.empty() || port.empty()) return false;
    
    // Connect
    struct timeval timeout = { 1, 500}; // 1.5 seconds
    redisContext* tconn = redisConnectWithTimeout(hostname.c_str(), atoi(port.c_str()), timeout);
    if (tconn == NULL || tconn->err) {
        if (tconn) {
            std::cerr << "Failed to connect with: " << hostname << ":" << port << " error : " << tconn->errstr << std::endl; 
            redisFree(tconn);
        } else {
            std::cerr << "Failed to allocate redis context for: " << hostname << ":" << port << std::endl; 
        }
        return false;
    }
    // Auth
    redisReply *reply;
    if (!auth.empty()) {
        if (!auth.empty()){
            std::string command_auth("Auth ");
            command_auth += auth;
            reply = reinterpret_cast< redisReply* >(redisCommand(tconn, command_auth.c_str()));
            if (reply == NULL || reply->type != REDIS_REPLY_STATUS || strcmp (reply->str, "OK") != 0) {
                std::cerr << "Auth Failed!" << std::endl;
                if (reply != NULL) freeReplyObject(reply);
                redisFree(tconn);
                return false;
            }
            freeReplyObject(reply);
        }
    }
    // Ping
    reply = reinterpret_cast< redisReply* >(redisCommand(tconn, "PING"));
    if (reply == NULL || strcmp(reply->str, "PONG") != 0){
        std::cerr << "PING Failed!" << std::endl;
        if (reply != NULL) freeReplyObject(reply);
        redisFree(tconn);
        return false;
    }
    freeReplyObject(reply);
    *conn = tconn;
    return true;
}

static bool reply_process(const redisReply *reply, redisContext *des_conn) {
    if (reply == NULL || des_conn == NULL) return false;
    std::cout << std::endl;
    std::cout << "Receive from source: " << reply->str << std::endl;

    std::vector<const char *> argv;
    std::vector<size_t> argv_len;
    bool ret = reply_parse(reply->str, reply->len, argv, argv_len);
    if (!ret){
        std::cerr << "Failed to parse reply: " << std::endl;
        return false;
    }
    
    // Sikp command
    if (!argv.empty() && (strcmp(argv[0], "AUTH") == 0 || strcmp(argv[0], "auth") == 0)) {
        std::cout << "Skip command 'AUTH'!"<< std::endl << std::endl;
        return true;
    }

    std::vector<const char *>::iterator it;
    redisReply *des_reply = reinterpret_cast< redisReply* >(redisCommandArgv(des_conn, argv.size(), &(argv[0]), &(argv_len[0])));
    if (des_reply == NULL) {
        std::cerr << "Failed to send to destination!"<< std::endl << std::endl;
        return false;
    }

    if (des_reply->type != REDIS_REPLY_ERROR) {
        std::cout << "Success transfor"<< std::endl << std::endl;
    } else {
        std::cerr << "Failed transfor : "<< des_reply->str << std::endl << std::endl;
    }
    
    for (it = argv.begin(); it != argv.end(); ++it)
    {
        free((void *)*it);
        *it = NULL;
    }

    freeReplyObject(des_reply);
    return true;
}

static bool option_parse(const std::string &str, std::string &host, std::string &port, std::string &auth) {
    if (str.empty()) return false;
    
    std::size_t at_i, colon_i;
    at_i = str.find_first_of('@');
    colon_i = str.find_first_of(':', at_i);
    auth.assign(str.substr(0, at_i));
    host.assign(str.substr(at_i + 1, colon_i - at_i - 1));
    port.assign(str.substr(colon_i + 1, str.size() - colon_i));
    return true;
}

static void usage()
{
    fprintf(stderr,
            "VERSION:%s \n"
            "DESCRIPTION:\n"
            " - Redis monitor copy tool: monitor redis server indicated by src_host, src_port, src_auth and send to des server\n"
            "Parameters: \n"
            "   -s: source server \n"
            "   -d: destination server \n"
            "   -v: show more information \n"
            "   -h: help \n"
            "Example: \n"
            " - ./redis-copy -s abc@xxx.xxx.xxx.xxx:6379 -d cba@xxx.xxx.xxx.xxx:6379 -v\n", VERSION
           );
}

int main(int argc, char **argv) {
    char c;
    std::string src, des;
    bool verbose = false;
    while (-1 != (c = getopt(argc, argv, "s:d:hv"))) {
        switch (c) {
            case 's':
                src.assign(optarg);
                break;
            case 'd':
                des.assign(optarg);
                break;
            case 'h':
                usage();
                break;
            case 'v':
                verbose = true;
                break;
            default:
                std::cerr << "Invalid option!" << std::endl;
                usage();
                return 0;
        }
    }

    std::string src_host, src_port, src_auth, des_host, des_port, des_auth;
    if ((!option_parse(src, src_host, src_port, src_auth)) || !option_parse(des, des_host, des_port, des_auth)) {
        std::cerr<< "Invaild parameter!" << std::endl;
        usage();
        return -1;
    }

    redisContext *src_conn = NULL, *des_conn = NULL;
    if (!remote_conn(&src_conn, src_host, src_port, src_auth)) {
        std::cerr << "Failed to connect to src server: " << src << std::endl;
        return -1;
    }
    if (!remote_conn(&des_conn, des_host, des_port, des_auth)) {
        std::cerr << "Failed to connect to des server: " << des << std::endl;
        return -1;
    }

    std::cout << "Monitor begin ...  " << std::endl;
    if (!verbose) {
        close(STDOUT_FILENO);
    }

    redisReply *src_reply = NULL;
    src_reply = reinterpret_cast< redisReply* >(redisCommand(src_conn, "Monitor"));
    if (src_reply != NULL) {
        std::cout << "Receive reply for Monitor: " << src_reply->str << std::endl;
        freeReplyObject(src_reply);
        while(redisGetReply(src_conn, (void **)&src_reply) == REDIS_OK) {
            bool res = reply_process(src_reply, des_conn);
            freeReplyObject(src_reply);
            if (!res) {
                redisFree(des_conn);
                redisFree(src_conn);
                return -1;
            }
        }
    }

    /* Disconnects and frees the context */
    redisFree(des_conn);
    redisFree(src_conn);

    return 0;
}
