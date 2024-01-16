/* $begin csapp.h */
#ifndef __CSAPP_H__
#  define __CSAPP_H__

#  include <arpa/inet.h>
#  include <fcntl.h>
#  include <netdb.h>
#  include <netinet/in.h>
#  include <pthread.h>
#  include <semaphore.h>
#  include <sys/mman.h>
#  include <sys/socket.h>
#  include <sys/stat.h>
#  include <sys/time.h>
#  include <sys/types.h>
#  include <sys/wait.h>
#  include <unistd.h>
#  include <cctype>
#  include <cerrno>
#  include <cmath>
#  include <csetjmp>
#  include <csignal>
#  include <cstdio>
#  include <cstdlib>
#  include <cstring>

/* Default file permissions are DEF_MODE & ~DEF_UMASK */
/* $begin createmasks */
#  define DEF_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)
#  define DEF_UMASK (S_IWGRP | S_IWOTH)
/* $end createmasks */

/* Simplifies calls to bind(), connect(), and accept() */
/* $begin sockaddrdef */
using SA = struct sockaddr;
/* $end sockaddrdef */

/* Persistent state for the robust I/O (Rio) package */
/* $begin rio_t */
#  define RIO_BUFSIZE 8192
using rio_t = struct {
  int rio_fd;                /* descriptor for this internal buf */
  int rio_cnt;               /* unread bytes in internal buf */
  char* rio_bufptr;          /* next unread byte in internal buf */
  char rio_buf[RIO_BUFSIZE]; /* internal buffer */
};
/* $end rio_t */

/* External variables */
extern char** environ; /* defined by libc */

/* Misc constants */
#  define MAXLINE 8192 /* max text line length */
#  define MAXBUF 8192  /* max I/O buffer size */
#  define LISTENQ 1024 /* second argument to listen() */

/* Process control wrappers */
pid_t Fork();
void Execve(const char* filename, char* const argv[], char* const envp[]);
pid_t Wait(int* status);
pid_t Waitpid(pid_t pid, int* iptr, int options);
void Kill(pid_t pid, int signum);
unsigned int Sleep(unsigned int secs);
void Pause();
unsigned int Alarm(unsigned int seconds);
void Setpgid(pid_t pid, pid_t pgid);
pid_t Getpgrp();

/* Signal wrappers */
using handler_t = void(int);
handler_t* Signal(int signum, handler_t* handler);
void Sigprocmask(int how, const sigset_t* set, sigset_t* oldset);
void Sigemptyset(sigset_t* set);
void Sigfillset(sigset_t* set);
void Sigaddset(sigset_t* set, int signum);
void Sigdelset(sigset_t* set, int signum);
int Sigismember(const sigset_t* set, int signum);

/* Unix I/O wrappers */
int Open(const char* pathname, int flags, mode_t mode);
ssize_t Read(int fd, void* buf, size_t count);
ssize_t Write(int fd, const void* buf, size_t count);
off_t Lseek(int fildes, off_t offset, int whence);
void Close(int fd);
int Select(int n, fd_set* readfds, fd_set* writefds, fd_set* exceptfds, struct timeval* timeout);
int Dup2(int fd1, int fd2);
void Stat(const char* filename, struct stat* buf);
void Fstat(int fd, struct stat* buf);

/* Memory mapping wrappers */
void* Mmap(void* addr, size_t len, int prot, int flags, int fd, off_t offset);
void Munmap(void* start, size_t length);

/* Standard I/O wrappers */
void Fclose(FILE* fp);
FILE* Fdopen(int fd, const char* type);
char* Fgets(char* ptr, int n, FILE* stream);
FILE* Fopen(const char* filename, const char* mode);
void Fputs(const char* ptr, FILE* stream);
size_t Fread(void* ptr, size_t size, size_t nmemb, FILE* stream);
void Fwrite(const void* ptr, size_t size, size_t nmemb, FILE* stream);

/* Dynamic storage allocation wrappers */
void* Malloc(size_t size);
void* Realloc(void* ptr, size_t size);
void* Calloc(size_t nmemb, size_t size);
void Free(void* ptr);

/* Sockets interface wrappers */
int Socket(int domain, int type, int protocol);
void Setsockopt(int s, int level, int optname, const void* optval, int optlen);
void Bind(int sockfd, struct sockaddr* my_addr, int addrlen);
void Listen(int s, int backlog);
int Accept(int s, struct sockaddr* addr, socklen_t* addrlen);
void Connect(int sockfd, struct sockaddr* serv_addr, int addrlen);

/* DNS wrappers */
struct hostent* Gethostbyname(const char* name);
struct hostent* Gethostbyaddr(const char* addr, int len, int type);

/* Pthreads thread control wrappers */
void Pthread_create(pthread_t* tidp, pthread_attr_t* attrp, void* (*routine)(void*), void* argp);
void Pthread_join(pthread_t tid, void** thread_return);
void Pthread_cancel(pthread_t tid);
void Pthread_detach(pthread_t tid);
void Pthread_exit(void* retval);
pthread_t Pthread_self();
void Pthread_once(pthread_once_t* once_control, void (*init_function)());

/* POSIX semaphore wrappers */
void Sem_init(sem_t* sem, int pshared, unsigned int value);
void P(sem_t* sem);
void V(sem_t* sem);

/* Rio (Robust I/O) package */
ssize_t rio_readn(int fd, void* usrbuf, size_t n);
ssize_t rio_writen(int fd, void* usrbuf, size_t n);
void rio_readinitb(rio_t* rp, int fd);
ssize_t rio_readnb(rio_t* rp, void* usrbuf, size_t n);
ssize_t rio_readlineb(rio_t* rp, void* usrbuf, size_t maxlen);

/* Wrappers for Rio package */
ssize_t Rio_readn(int fd, void* ptr, size_t n);
void Rio_writen(int fd, void* usrbuf, size_t n);
void Rio_readinitb(rio_t* rp, int fd);
ssize_t Rio_readnb(rio_t* rp, void* usrbuf, size_t n);
ssize_t Rio_readlineb(rio_t* rp, void* usrbuf, size_t maxlen);

/* Client/server helper functions */
int open_clientfd(char* hostname, int portno);
int open_listenfd(int portno);

/* Wrappers for client/server helper functions */
int Open_clientfd(char* hostname, int port);
int Open_listenfd(int port);

#endif /* __CSAPP_H__ */
/* $end csapp.h */
