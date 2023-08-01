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
  int32_t rio_fd;                /* descriptor for this internal buf */
  int32_t rio_cnt;               /* unread bytes in internal buf */
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
pid_t Wait(int32_t* status);
pid_t Waitpid(pid_t pid, int32_t* iptr, int32_t options);
void Kill(pid_t pid, int32_t signum);
uint32_t Sleep(uint32_t secs);
void Pause();
uint32_t Alarm(uint32_t seconds);
void Setpgid(pid_t pid, pid_t pgid);
pid_t Getpgrp();

/* Signal wrappers */
using handler_t = void (int32_t);
handler_t* Signal(int32_t signum, handler_t* handler);
void Sigprocmask(int32_t how, const sigset_t* set, sigset_t* oldset);
void Sigemptyset(sigset_t* set);
void Sigfillset(sigset_t* set);
void Sigaddset(sigset_t* set, int32_t signum);
void Sigdelset(sigset_t* set, int32_t signum);
int32_t Sigismember(const sigset_t* set, int32_t signum);

/* Unix I/O wrappers */
int32_t Open(const char* pathname, int32_t flags, mode_t mode);
ssize_t Read(int32_t fd, void* buf, size_t count);
ssize_t Write(int32_t fd, const void* buf, size_t count);
off_t Lseek(int32_t fildes, off_t offset, int32_t whence);
void Close(int32_t fd);
int32_t Select(int32_t n, fd_set* readfds, fd_set* writefds, fd_set* exceptfds, struct timeval* timeout);
int32_t Dup2(int32_t fd1, int32_t fd2);
void Stat(const char* filename, struct stat* buf);
void Fstat(int32_t fd, struct stat* buf);

/* Memory mapping wrappers */
void* Mmap(void* addr, size_t len, int32_t prot, int32_t flags, int32_t fd, off_t offset);
void Munmap(void* start, size_t length);

/* Standard I/O wrappers */
void Fclose(FILE* fp);
FILE* Fdopen(int32_t fd, const char* type);
char* Fgets(char* ptr, int32_t n, FILE* stream);
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
int32_t Socket(int32_t domain, int32_t type, int32_t protocol);
void Setsockopt(int32_t s, int32_t level, int32_t optname, const void* optval, int32_t optlen);
void Bind(int32_t sockfd, struct sockaddr* my_addr, int32_t addrlen);
void Listen(int32_t s, int32_t backlog);
int32_t Accept(int32_t s, struct sockaddr* addr, socklen_t* addrlen);
void Connect(int32_t sockfd, struct sockaddr* serv_addr, int32_t addrlen);

/* DNS wrappers */
struct hostent* Gethostbyname(const char* name);
struct hostent* Gethostbyaddr(const char* addr, int32_t len, int32_t type);

/* Pthreads thread control wrappers */
void Pthread_create(pthread_t* tidp, pthread_attr_t* attrp, void* (*routine)(void*), void* argp);
void Pthread_join(pthread_t tid, void** thread_return);
void Pthread_cancel(pthread_t tid);
void Pthread_detach(pthread_t tid);
void Pthread_exit(void* retval);
pthread_t Pthread_self();
void Pthread_once(pthread_once_t* once_control, void (*init_function)());

/* POSIX semaphore wrappers */
void Sem_init(sem_t* sem, int32_t pshared, uint32_t value);
void P(sem_t* sem);
void V(sem_t* sem);

/* Rio (Robust I/O) package */
ssize_t rio_readn(int32_t fd, void* usrbuf, size_t n);
ssize_t rio_writen(int32_t fd, void* usrbuf, size_t n);
void rio_readinitb(rio_t* rp, int32_t fd);
ssize_t rio_readnb(rio_t* rp, void* usrbuf, size_t n);
ssize_t rio_readlineb(rio_t* rp, void* usrbuf, size_t maxlen);

/* Wrappers for Rio package */
ssize_t Rio_readn(int32_t fd, void* ptr, size_t n);
void Rio_writen(int32_t fd, void* usrbuf, size_t n);
void Rio_readinitb(rio_t* rp, int32_t fd);
ssize_t Rio_readnb(rio_t* rp, void* usrbuf, size_t n);
ssize_t Rio_readlineb(rio_t* rp, void* usrbuf, size_t maxlen);

/* Client/server helper functions */
int32_t open_clientfd(char* hostname, int32_t portno);
int32_t open_listenfd(int32_t portno);

/* Wrappers for client/server helper functions */
int32_t Open_clientfd(char* hostname, int32_t port);
int32_t Open_listenfd(int32_t port);

#endif /* __CSAPP_H__ */
/* $end csapp.h */
