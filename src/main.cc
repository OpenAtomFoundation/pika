#include "pika.h"
#include "xdebug.h"
#include <glog/logging.h>
#include "pika_conf.h"
#include "pika_hb.h"


#include <iostream>
#include <signal.h>

Pika *gPika;

PikaConf *gPikaConf;

static void pika_glog_init()
{
    FLAGS_log_dir = gPikaConf->log_path();
    ::google::InitGoogleLogging("pika");
    FLAGS_minloglevel = gPikaConf->log_level();
    LOG(WARNING) << "Pika glog init";
    /*
     * dump some usefull message when crash on certain signals
     */
    // google::InstallFailureSignalHandler();
}

static void sig_handler(const int sig)
{
    printf("Caught signal %d", sig);
}

void pika_signal_setup()
{
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    LOG(WARNING) << "pika signal setup ok";
}


static void version()
{
    printf("-----------Pika server 1.0.0----------\n");
}
void pika_init_conf(const char* path)
{
    gPikaConf = new PikaConf(path);
    if (gPikaConf == NULL) {
        LOG(FATAL) << "pika load conf error";
    }

    version();
    printf("-----------Pika config list----------\n");
    gPikaConf->DumpConf();
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
    bool pathOpt = false;
    char c;
    char path[PIKA_LINE_SIZE];
    if (argc != 2) {
        usage();
        return -1;
    }
    while (-1 != (c = getopt(argc, argv, "D:h"))) {
        switch (c) {
        case 'D':
            snprintf(path, PIKA_LINE_SIZE, "%s", optarg);
            pathOpt = 1;
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
    if (pathOpt == false) {
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
     * Init the pika
	 * Inside the pika, we have pikaWorker, pikaHb
     */
    gPika = new Pika();
    
    if (gPika != NULL) {
        LOG(WARNING) << "Pika Server init ok";
    } else {
        LOG(FATAL) << "Pika Server init error";
    }


    /*
     * Before now, all we do is initial all server
     * Then we will start our server in order
     */
    log_info("Start running heartbeat");

	/*
	 * Start the heartbeat module
	 */
	gPika->RunHb();

    /*
     * gPika->RunPulse();
     */


    /*
     * Start main worker, the main server must be the last one to start
     * because we must prepare and check everything before we can serve
     */
    LOG(WARNING) << "Pika Server going to start";
    log_info("Start running pika main server");
    gPika->RunWorker();

    return 0;
}
