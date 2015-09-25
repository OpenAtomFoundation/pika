#include <iostream>
#include <string>
#include <queue>
#include <sys/time.h>

#include "mario.h"
#include "consumer.h"
#include "mutexlock.h"
#include "port.h"
#include "version.h"
#include "env.h"
#include "mario.h"

using namespace mario;
int main(int argc, char** argv)
{
    if (argc < 2) {
        exit(-1);
    }
    char *path = argv[1];

    Status s;
//    const char *path = "/home/chenzongzhi/git/mario/log/manifest";
    Env* env = Env::Default();
    RWFile *versionfile;
    const std::string spath(path, strlen(path));
    env->NewRWFile(spath, &versionfile);
    Version *v = new Version(versionfile);
    v->InitSelf();
    if (s.ok()) {
        log_info("pro_offset %llu con_offset %llu itemnum %u pronum %u connum %u", v->pro_offset(), v->con_offset(), v->item_num(), v->pronum(), v->connum());
    } else {
        log_warn("init error");
    }

    return 0;
}
