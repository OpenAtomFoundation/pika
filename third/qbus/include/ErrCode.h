#ifndef __ERRCODE_H__
#define __ERRCODE_H__

class ErrCode
{
    public:
    static const int OK;                              // operation OK.
    static const int ERR_ZK_SET;                      // zookeeper api zoo_set failed.
    static const int ERR_ZK_GET;                      // zookeeper api zoo_get failed.
    static const int ERR_ZK_EXISTS;                   // zookeeper api zoo_exists failed.
    static const int ERR_ZK_DELETE;                   // zookeeper api zoo_delete failed.
    static const int ERR_ZK_CREATE;                   // zookeeper api zoo_create failed.
    static const int ERR_ZK_GET_CHILDREN;             // zookeeper api zoo_get_children failed.
    static const int ERR_ZK_NO_PARENT; // parent node of the node to be operated on non-exists
    static const int ERR_ZK_BAD_PATH;
    static const int ERR_ZK_CREATE_EXISTS;

    static const int ERR_DATA_FORMAT;
    static const int ERR_NODE_NOT_EXISTS;             // zookeeper api zoo_exists failed.
    static const int ERR_MALLOC_MEM;                  // malloc failed.
    static const int ERR_RELEASE_WRONG_PARTITION;
    static const int ERR_REGISTER_PART_OWNER;
    static const int ERR_ZK_OP;                       //operate zookeeper failed, maybe network reason

    static const int ERR_OFFSET;                      //get wrong offset from zookeeper
    static const int ERR_INVALID_MSG;                 //invalid msg for wrong checksum
    static const int ERR_CONNECTION;                  //connect to server failed
    static const int ERR_ENCODE_MSG;                  //encode message failed
    static const int ERR_GET_FLOCK;                   //get file lock failed when sending message
    static const int ERR_WRITE_FILE;                  //write file failed when sending message
    static const int ERR_OPEN_FILE;
    static const int ERR_READ_REQUEST_SIZE;

    static const int ERR_SOCKET;
    static const int ERR_SOCK_CONNECT;
    static const int ERR_SOCK_RECV;
    static const int ERR_SOCK_SEND;
};

#endif


