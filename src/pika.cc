#include "pika_server.h"
#include "xdebug.h"
#include <glog/logging.h>
#include "pika_conf.h"
#include "pika_command.h"


#include <iostream>
#include <signal.h>
#include <map>

PikaConf *g_pikaConf;

PikaServer *g_pikaServer;


std::map<std::string, Cmd *> g_pikaCmd;


static void pika_glog_init()
{
    FLAGS_log_dir = g_pikaConf->log_path();
    ::google::InitGoogleLogging("pika");
    FLAGS_minloglevel = g_pikaConf->log_level();
    FLAGS_alsologtostderr = true;
    LOG(WARNING) << "Pika glog init";
    /*
     * dump some usefull message when crash on certain signals
     */
    // google::InstallFailureSignalHandler();
}

static void sig_handler(const int sig)
{
    LOG(INFO) << "Caught signal " << sig;
    ::google::ShutdownGoogleLogging();
    exit(1);
}

void pika_signal_setup()
{
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, &sig_handler);
//    struct sigaction sigIntHandler;
//    sigIntHandler.sa_handler = sig_handler;
//    sigemptyset(&sigIntHandler.sa_mask);
//    sigIntHandler.sa_flags = 0;
//    sigaction(SIGINT, &sigIntHandler, NULL);
    LOG(WARNING) << "pika signal setup ok";
}


static void version()
{
    printf("-----------Pika server 1.0.0----------\n");
}
void pika_init_conf(const char* path)
{
    g_pikaConf = new PikaConf(path);
    if (g_pikaConf == NULL) {
        LOG(FATAL) << "pika load conf error";
    }

    version();
    printf("-----------Pika config list----------\n");
    g_pikaConf->DumpConf();
    printf("-----------Pika config end----------\n");
}


static void usage()
{
    fprintf(stderr,
            "Pika module 1.0.0\n"
            "need One parameters\n"
            "-D the conf path \n"
            "-h show this usage message \n"
            "example: ./bin/pika -D./conf/pika.conf\n"
           );
}

int main(int argc, char **argv)
{
    bool path_opt = false;
    char c;
    char path[PIKA_LINE_SIZE];

    while (-1 != (c = getopt(argc, argv, "D:h"))) {
        switch (c) {
        case 'D':
            snprintf(path, PIKA_LINE_SIZE, "%s", optarg);
            path_opt = 1;
            break;
        case 'h':
            usage();
            return 0;
        default:
            usage();
            return 0;
        }
    }

    /*
     * check wether set the conf path
     */
    if (path_opt == false) {
        LOG(FATAL) << "Don't get the conf path";
    }

    /*
     * init the conf
     */
    pika_init_conf(path);

    /*
     * init the glog config
     */
    pika_glog_init();

    /*
     * set up the signal
     */
    pika_signal_setup();


    /*
     * admin
     */
    AuthCmd *authptr = new AuthCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("auth", authptr));
    PingCmd *pingptr = new PingCmd(1);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("ping", pingptr));
    ClientCmd *clientptr = new ClientCmd(-1);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("client", clientptr));
    ConfigCmd *configptr = new ConfigCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("config", configptr));

    /*
     * kv
     */
    SetCmd *setptr = new SetCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("set", setptr));
    GetCmd *getptr = new GetCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("get", getptr));
    DelCmd *delptr = new DelCmd(-2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("del", delptr));
    IncrCmd *incrptr = new IncrCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("incr", incrptr));
    IncrbyCmd *incrbyptr = new IncrbyCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("incrby", incrbyptr));
    IncrbyfloatCmd *incrbyfloatptr = new IncrbyfloatCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("incrbyfloat", incrbyfloatptr));
    DecrCmd *decrptr = new DecrCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("decr", decrptr));
    DecrbyCmd *decrbyptr = new DecrbyCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("decrby", decrbyptr));
    GetsetCmd *getsetptr = new GetsetCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("getset", getsetptr));
    AppendCmd *appendptr = new AppendCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("append", appendptr));
    MgetCmd *mgetptr = new MgetCmd(-2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("mget", mgetptr));
    SetnxCmd *setnxptr = new SetnxCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("setnx", setnxptr));
    SetexCmd *setexptr = new SetexCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("setex", setexptr));
    MsetCmd *msetptr = new MsetCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("mset", msetptr));
    MsetnxCmd *msetnxptr = new MsetnxCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("msetnx", msetnxptr));
    GetrangeCmd *getrangeptr = new GetrangeCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("getrange", getrangeptr));
    SetrangeCmd *setrangeptr = new SetrangeCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("setrange", setrangeptr));
    StrlenCmd *strlenptr = new StrlenCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("strlen", strlenptr));
    ExistsCmd *existsptr = new ExistsCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("exists", existsptr));
    ExpireCmd *expireptr = new ExpireCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("expire", expireptr));
    ExpireatCmd *expireatptr = new ExpireatCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("expireat", expireatptr));
    TtlCmd *ttlptr = new TtlCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("ttl", ttlptr));
    PersistCmd *persistptr = new PersistCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("persist", persistptr));
    ScanCmd *scanptr = new ScanCmd(-2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("scan", scanptr));

    /*
     * hash
     */
    HSetCmd *hsetptr = new HSetCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hset", hsetptr));
    HGetCmd *hgetptr = new HGetCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hget", hgetptr));
    HDelCmd *hdelptr = new HDelCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hdel", hdelptr));
    HExistsCmd *hexistsptr = new HExistsCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hexists", hexistsptr));
    HGetallCmd *hgetallptr = new HGetallCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hgetall", hgetallptr));
    HIncrbyCmd *hincrbyptr = new HIncrbyCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hincrby", hincrbyptr));
    HIncrbyfloatCmd *hincrbyfloatptr = new HIncrbyfloatCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hincrbyfloat", hincrbyfloatptr));
    HKeysCmd *hkeysptr = new HKeysCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hkeys", hkeysptr));
    HLenCmd *hlenptr = new HLenCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hlen", hlenptr));
    HMSetCmd *hmsetptr = new HMSetCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hmset", hmsetptr));
    HMGetCmd *hmgetptr = new HMGetCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hmget", hmgetptr));
    HSetnxCmd *hsetnxptr = new HSetnxCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hsetnx", hsetnxptr));
    HValsCmd *hvalsptr = new HValsCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hvals", hvalsptr));
    HStrlenCmd *hstrlenptr = new HStrlenCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hstrlen", hstrlenptr));
    HScanCmd *hscanptr = new HScanCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("hscan", hscanptr));

    /*
     * lists
     */
    LIndexCmd *lindexptr = new LIndexCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("lindex", lindexptr));
    LInsertCmd *linsertptr = new LInsertCmd(5);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("linsert", linsertptr));
    LLenCmd *llenptr = new LLenCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("llen", llenptr));
    LPopCmd *lpopptr = new LPopCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("lpop", lpopptr));
    LPushCmd *lpushptr = new LPushCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("lpush", lpushptr));
    LPushxCmd *lpushxptr = new LPushxCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("lpushx", lpushxptr));
    LRangeCmd *lrangeptr = new LRangeCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("lrange", lrangeptr));
    LRemCmd *lremptr = new LRemCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("lrem", lremptr));
    LSetCmd *lsetptr = new LSetCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("lset", lsetptr));
    LTrimCmd *ltrimptr = new LTrimCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("ltrim", ltrimptr));
    RPopCmd *rpopptr = new RPopCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("rpop", rpopptr));
    RPopLPushCmd *rpoplpushptr = new RPopLPushCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("rpoplpush", rpoplpushptr));
    RPushCmd *rpushptr = new RPushCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("rpush", rpushptr));
    RPushxCmd *rpushxptr = new RPushxCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("rpushx", rpushxptr));

    /*
     * lists
     */
    ZAddCmd *zaddptr = new ZAddCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zadd", zaddptr));
    ZCardCmd *zcardptr = new ZCardCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zcard", zcardptr));
    ZScanCmd *zscanptr = new ZScanCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zscan", zscanptr));
    ZIncrbyCmd *zincrbyptr = new ZIncrbyCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zincrby", zincrbyptr));
    ZRangeCmd *zrangeptr = new ZRangeCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrange", zrangeptr));
    ZRangebyscoreCmd *zrangebyscoreptr = new ZRangebyscoreCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrangebyscore", zrangebyscoreptr));
    ZCountCmd *zcountptr = new ZCountCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zcount", zcountptr));
    ZRemCmd *zremptr = new ZRemCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrem", zremptr));
    ZUnionstoreCmd *zunionstoreptr = new ZUnionstoreCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zunionstore", zunionstoreptr));
    ZInterstoreCmd *zinterstoreptr = new ZInterstoreCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zinterstore", zinterstoreptr));
    ZRankCmd *zrankptr = new ZRankCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrank", zrankptr));
    ZRevrankCmd *zrevrankptr = new ZRevrankCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrevrank", zrevrankptr));
    ZScoreCmd *zscoreptr = new ZScoreCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zscore", zscoreptr));
    ZRevrangeCmd *zrevrangeptr = new ZRevrangeCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrevrange", zrevrangeptr));
    ZRevrangebyscoreCmd *zrevrangebyscoreptr = new ZRevrangebyscoreCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrevrangebyscore", zrevrangebyscoreptr));
    ZRangebylexCmd *zrangebylexptr = new ZRangebylexCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zrangebylex", zrangebylexptr));
    ZLexcountCmd *zlexcountptr = new ZLexcountCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zlexcount", zlexcountptr));
    ZRemrangebylexCmd *zremrangebylexptr = new ZRemrangebylexCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zremrangebylex", zremrangebylexptr));
    ZRemrangebyrankCmd *zremrangebyrankptr = new ZRemrangebyrankCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zremrangebyrank", zremrangebyrankptr));
    ZRemrangebyscoreCmd *zremrangebyscoreptr = new ZRemrangebyscoreCmd(4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("zremrangebyscore", zremrangebyscoreptr));

    /*
     * set
     */
    SAddCmd *saddptr = new SAddCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sadd", saddptr));
    SRemCmd *sremptr = new SRemCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("srem", sremptr));
    SCardCmd *scardptr = new SCardCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("scard", scardptr));
    SMembersCmd *smembersptr = new SMembersCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("smembers", smembersptr));
    SScanCmd *sscanptr = new SScanCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sscan", sscanptr));
    SUnionCmd *sunionptr = new SUnionCmd(-2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sunion", sunionptr));
    SUnionstoreCmd *sunionstoreptr = new SUnionstoreCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sunionstore", sunionstoreptr));
    SInterCmd *sinterptr = new SInterCmd(-2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sinter", sinterptr));
    SInterstoreCmd *sinterstoreptr = new SInterstoreCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sinterstore", sinterstoreptr));
    SIsmemberCmd *sismemberptr = new SIsmemberCmd(3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sismember", sismemberptr));
    SDiffCmd *sdiffptr = new SDiffCmd(-2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sdiff", sdiffptr));
    SDiffstoreCmd *sdiffstoreptr = new SDiffstoreCmd(-3);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("sdiffstore", sdiffstoreptr));
    SMoveCmd *smoveptr = new SMoveCmd(-4);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("smove", smoveptr));
    SPopCmd *spopptr = new SPopCmd(2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("spop", spopptr));
    SRandmemberCmd *srandmemberptr = new SRandmemberCmd(-2);
    g_pikaCmd.insert(std::pair<std::string, Cmd *>("srandmember", srandmemberptr));

    /*
     * Init the server
     */
    g_pikaServer = new PikaServer();
    
    if (g_pikaServer != NULL) {
        LOG(WARNING) << "Pika Server init ok";
    } else {
        LOG(FATAL) << "Pika Server init error";
    }


    LOG(WARNING) << "Pika Server going to start";
    g_pikaServer->RunProcess();

    return 0;
}
