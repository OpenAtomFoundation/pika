#include "pstd/include/posix.h"
#include "pstd/include/xdebug.h"

#include <glog/logging.h>
#include <strings.h>
#include <cstring>
/*********************************************
 * Wrappers for Unix process control functions
 ********************************************/

/* $begin forkwrapper */
pid_t Fork() {
  pid_t pid;

  if ((pid = fork()) < 0) {
    LOG(ERROR) << "Fork error: " << strerror(errno);
  }
  return pid;
}
/* $end forkwrapper */

void Execve(const char* filename, char* const argv[], char* const envp[]) {
  if (execve(filename, argv, envp) < 0) {
    LOG(ERROR) << "Execve error: " << strerror(errno);
  }
}

/* $begin wait */
pid_t Wait(int* status) {
  pid_t pid;

  if ((pid = wait(status)) < 0) {
    LOG(ERROR) << "Wait error: " << strerror(errno);
  }
  return pid;
}
/* $end wait */

pid_t Waitpid(pid_t pid, int* iptr, int options) {
  pid_t retpid;

  if ((retpid = waitpid(pid, iptr, options)) < 0) {
    LOG(ERROR) << "Waitpid error: " << strerror(errno);
  }
  return (retpid);
}

/* $begin kill */
void Kill(pid_t pid, int signum) {
  int rc;

  if ((rc = kill(pid, signum)) < 0) {
    LOG(ERROR) << "Kill error: " << strerror(errno);
  }
}
/* $end kill */

void Pause() {
  (void)pause();
}

unsigned int Sleep(unsigned int secs) { return sleep(secs); }

unsigned int Alarm(unsigned int seconds) { return alarm(seconds); }

void Setpgid(pid_t pid, pid_t pgid) {
  int rc;

  if ((rc = setpgid(pid, pgid)) < 0) {
    LOG(ERROR) << "Setpgid error: " << strerror(errno);
  }
  }

pid_t Getpgrp() { return getpgrp(); }

/************************************
 * Wrappers for Unix signal functions
 ***********************************/

/* $begin sigaction */
handler_t* Signal(int signum, handler_t* handler) {
  struct sigaction action;
  struct sigaction old_action;

  action.sa_handler = handler;
  sigemptyset(&action.sa_mask); /* block sigs of type being handled */
  action.sa_flags = SA_RESTART; /* restart syscalls if possible */

  if (sigaction(signum, &action, &old_action) < 0) {
    LOG(ERROR) << "Signal error: " << strerror(errno);
  }
  return (old_action.sa_handler);
}
/* $end sigaction */

void Sigprocmask(int how, const sigset_t* set, sigset_t* oldset) {
  if (sigprocmask(how, set, oldset) < 0) {
    LOG(ERROR) << "Sigprocmask error: " << strerror(errno);
  }
  }

void Sigemptyset(sigset_t* set) {
  if (sigemptyset(set) < 0) {
    LOG(ERROR) << "Sigemptyset error: " << strerror(errno);
  }
  }

void Sigfillset(sigset_t* set) {
  if (sigfillset(set) < 0) {
    LOG(ERROR) << "Sigfillset error: " << strerror(errno);
  }
  }

void Sigaddset(sigset_t* set, int signum) {
  if (sigaddset(set, signum) < 0) {
    LOG(ERROR) << "Sigaddset error: " << strerror(errno);
  }
  }

void Sigdelset(sigset_t* set, int signum) {
  if (sigdelset(set, signum) < 0) {
    LOG(ERROR) << "Sigdelset error: " << strerror(errno);
  }
  }

int Sigismember(const sigset_t* set, int signum) {
  int rc;
  if (rc = sigismember(set, signum); rc < 0) {
    LOG(ERROR) << "Sigismember error: " << strerror(errno);
  }
  return rc;
}

/********************************
 * Wrappers for Unix I/O routines
 ********************************/

int Open(const char* pathname, int flags, mode_t mode) {
  int rc;

  if ((rc = open(pathname, flags, mode)) < 0) {
    LOG(ERROR) << "Open error: " << strerror(errno);
  }
  return rc;
}

ssize_t Read(int fd, void* buf, size_t count) {
  ssize_t rc;

  if ((rc = read(fd, buf, count)) < 0) {
    LOG(ERROR) << "Read error: " << strerror(errno);
  }
  return rc;
}

ssize_t Write(int fd, const void* buf, size_t count) {
  ssize_t rc;

  if ((rc = write(fd, buf, count)) < 0) {
    LOG(ERROR) << "Write error: " << strerror(errno);
  }
  return rc;
}

off_t Lseek(int fildes, off_t offset, int whence) {
  off_t rc;

  if ((rc = lseek(fildes, offset, whence)) < 0) {
    LOG(ERROR) << "Lseek error: " << strerror(errno);
  }
  return rc;
}

void Close(int fd) {
  int rc;

  if ((rc = close(fd)) < 0) {
    LOG(ERROR) << "Close error: " << strerror(errno);
  }
}

int Select(int n, fd_set* readfds, fd_set* writefds, fd_set* exceptfds, struct timeval* timeout) {
  int rc;

  if ((rc = select(n, readfds, writefds, exceptfds, timeout)) < 0) {
    LOG(ERROR) << "Select error: " << strerror(errno);
  }
  return rc;
}

int Dup2(int fd1, int fd2) {
  int rc;

  if ((rc = dup2(fd1, fd2)) < 0) {
    LOG(ERROR) << "Dup2 error: " << strerror(errno);
  }
  return rc;
}

void Stat(const char* filename, struct stat* buf) {
  if (stat(filename, buf) < 0) {
    LOG(ERROR) << "Stat error: " << strerror(errno);
  }
}

void Fstat(int fd, struct stat* buf) {
  if (fstat(fd, buf) < 0) {
    LOG(ERROR) << "Fstat error: " << strerror(errno);
  }
}

/***************************************
 * Wrappers for memory mapping functions
 ***************************************/
void* Mmap(void* addr, size_t len, int prot, int flags, int fd, off_t offset) {
  void* ptr;

  if ((ptr = mmap(addr, len, prot, flags, fd, offset)) == ((void*)-1)) {  // NOLINT
    LOG(ERROR) << "mmap error: " << strerror(errno);
  }
  return (ptr);
}

void Munmap(void* start, size_t length) {
  if (munmap(start, length) < 0) {
    LOG(ERROR) << "munmap error: " << strerror(errno);
  }
}

/***************************************************
 * Wrappers for dynamic storage allocation functions
 ***************************************************/

void* Malloc(size_t size) {
  void* p;

  if ((p = malloc(size)) == nullptr) {
    LOG(ERROR) << "Malloc error: " << strerror(errno);
  }
  return p;
}

void* Realloc(void* ptr, size_t size) {
  void* p;

  if ((p = realloc(ptr, size)) == nullptr) {
    LOG(ERROR) << "Realloc error: " << strerror(errno);
  }
  return p;
}

void* Calloc(size_t nmemb, size_t size) {
  void* p;

  if ((p = calloc(nmemb, size)) == nullptr) {
    LOG(ERROR) << "Calloc error: " << strerror(errno);
  }
  return p;
}

void Free(void* ptr) { free(ptr); }

/******************************************
 * Wrappers for the Standard I/O functions.
 ******************************************/
void Fclose(FILE* fp) {
  if (fclose(fp) != 0) {
    LOG(ERROR) << "Fclose error: " << strerror(errno);
  }
}

FILE* Fdopen(int fd, const char* type) {
  FILE* fp;

  if ((fp = fdopen(fd, type)) == nullptr) {
    LOG(ERROR) << "Fdopen error: " << strerror(errno);
  }

  return fp;
}

char* Fgets(char* ptr, int n, FILE* stream) {
  char* rptr;

  if (((rptr = fgets(ptr, n, stream)) == nullptr) && (ferror(stream) != 0)) {
    LOG(ERROR) << "Fgets error";
  }

  return rptr;
}

FILE* Fopen(const char* filename, const char* mode) {
  FILE* fp;

  if ((fp = fopen(filename, mode)) == nullptr) {
    LOG(ERROR) << "Fopen error: " << strerror(errno);
  }

  return fp;
}

void Fputs(const char* ptr, FILE* stream) {
  if (fputs(ptr, stream) == EOF) {
    LOG(ERROR) << "Fputs error: " << strerror(errno);
  }
}

size_t Fread(void* ptr, size_t size, size_t nmemb, FILE* stream) {
  size_t n;

  if (((n = fread(ptr, size, nmemb, stream)) < nmemb) && (ferror(stream) != 0)) {
    LOG(ERROR) << "Fread error: " << strerror(errno);
  }
  return n;
}

void Fwrite(const void* ptr, size_t size, size_t nmemb, FILE* stream) {
  if (fwrite(ptr, size, nmemb, stream) < nmemb) {
    LOG(ERROR) << "Fwrite error: " << strerror(errno);
  }
}

/****************************
 * Sockets interface wrappers
 ****************************/

int Socket(int domain, int type, int protocol) {
  int rc;

  if ((rc = socket(domain, type, protocol)) < 0) {
    LOG(ERROR) << "Socket error: " << strerror(errno);
  }
  return rc;
}

void Setsockopt(int s, int level, int optname, const void* optval, int optlen) {
  if (setsockopt(s, level, optname, optval, optlen) < 0) {
    LOG(ERROR) << "Setsockopt error: " << strerror(errno);
  }
}

void Bind(int sockfd, struct sockaddr* my_addr, int addrlen) {
  if (bind(sockfd, my_addr, addrlen) < 0) {
    LOG(ERROR) << "Bind error: " << strerror(errno);
  }
}

void Listen(int s, int backlog) {
  if (listen(s, backlog) < 0) {
    LOG(ERROR) << "Listen error: " << strerror(errno);
  }
}

int Accept(int s, struct sockaddr* addr, socklen_t* addrlen) {
  int rc;

  if (rc = accept(s, addr, addrlen); rc < 0) {
    LOG(ERROR) << "Accept error: " << strerror(errno);
  }
  return rc;
}

void Connect(int sockfd, struct sockaddr* serv_addr, int addrlen) {
  if (connect(sockfd, serv_addr, addrlen) < 0) {
    LOG(ERROR) << "Connect error: " << strerror(errno);
  }
}

/************************
 * DNS interface wrappers
 ***********************/

/* $begin gethostbyname */
struct hostent* Gethostbyname(const char* name) {
  struct hostent* p;

  if ((p = gethostbyname(name)) == nullptr) {
    LOG(ERROR) << "Gethostbyname error: DNS error " << h_errno;
  }
  return p;
}
/* $end gethostbyname */

struct hostent* Gethostbyaddr(const char* addr, int len, int type) {
  struct hostent* p;

  if (p = gethostbyaddr(addr, len, type); p == nullptr) {
    LOG(ERROR) << "Gethostbyaddr error: DNS error " << h_errno;
  }
  return p;
}

/************************************************
 * Wrappers for Pthreads thread control functions
 ************************************************/

void Pthread_create(pthread_t* tidp, pthread_attr_t* attrp, void* (*routine)(void*), void* argp) {
  int rc;

  if (rc = pthread_create(tidp, attrp, routine, argp); rc != 0) {
    LOG(ERROR) << "Pthread_create error: " << strerror(rc);
  }
}

void Pthread_cancel(pthread_t tid) {
  int rc;

  if (rc = pthread_cancel(tid); rc != 0) {
    LOG(ERROR) << "Pthread_cancel error: " << strerror(rc);
  }
}

void Pthread_join(pthread_t tid, void** thread_return) {
  int rc;

  if ((rc = pthread_join(tid, thread_return)) != 0) {
    LOG(ERROR) << "Pthread_join error: " << strerror(rc);
  }
}

/* $begin detach */
void Pthread_detach(pthread_t tid) {
  int rc;

  if ((rc = pthread_detach(tid)) != 0) {
    LOG(ERROR) << "Pthread_detach error: " << strerror(rc);
  }
}
/* $end detach */

void Pthread_exit(void* retval) { pthread_exit(retval); }

pthread_t Pthread_self() { return pthread_self(); }

void Pthread_once(pthread_once_t* once_control, void (*init_function)()) { pthread_once(once_control, init_function); }

/*******************************
 * Wrappers for Posix semaphores
 *******************************/

void Sem_init(sem_t* sem, int pshared, unsigned int value) {
// TODO(clang-tidy) : should use c11 cond or mutex instead of Posix sem
  if (sem_init(sem, pshared, value) < 0) {  // NOLINT
    LOG(ERROR) << "Sem_init error: " << strerror(errno);
  }
}

void P(sem_t* sem) {
  if (sem_wait(sem) < 0) {
    LOG(ERROR) << "P error: " << strerror(errno);
  }
}

void V(sem_t* sem) {
  if (sem_post(sem) < 0) {
    LOG(ERROR) << "V error: " << strerror(errno);
  }
}

/*********************************************************************
 * The Rio package - robust I/O functions
 **********************************************************************/
/*
 * rio_readn - robustly read n bytes (unbuffered)
 */
/* $begin rio_readn */
ssize_t rio_readn(int fd, void* usrbuf, size_t n) {
  size_t nleft = n;
  ssize_t nread;
  char* bufp = static_cast<char*>(usrbuf);

  while (nleft > 0) {
    if ((nread = read(fd, bufp, nleft)) < 0) {
      if (errno == EINTR) { /* interrupted by sig handler return */
        nread = 0;        /* and call read() again */
      } else {
        return -1; /* errno set by read() */
}
    } else if (nread == 0) {
      break; /* EOF */
}
    nleft -= nread;
    bufp += nread;
  }
  return (n - nleft); /* return >= 0 */
}
/* $end rio_readn */

/*
 * rio_writen - robustly write n bytes (unbuffered)
 */
/* $begin rio_writen */
ssize_t rio_writen(int fd, void* usrbuf, size_t n) {
  size_t nleft = n;
  ssize_t nwritten;
  char* bufp = static_cast<char*>(usrbuf);

  while (nleft > 0) {
    if ((nwritten = write(fd, bufp, nleft)) <= 0) {
      if (errno == EINTR) { /* interrupted by sig handler return */
        nwritten = 0;     /* and call write() again */
      } else {
        return -1; /* errorno set by write() */
}
    }
    nleft -= nwritten;
    bufp += nwritten;
  }
  return n;
}
/* $end rio_writen */

/*
 * rio_read - This is a wrapper for the Unix read() function that
 *    transfers min(n, rio_cnt) bytes from an internal buffer to a user
 *    buffer, where n is the number of bytes requested by the user and
 *    rio_cnt is the number of unread bytes in the internal buffer. On
 *    entry, rio_read() refills the internal buffer via a call to
 *    read() if the internal buffer is empty.
 */
/* $begin rio_read */
static ssize_t rio_read(rio_t* rp, char* usrbuf, size_t n) {
  int cnt;

  while (rp->rio_cnt <= 0) { /* refill if buf is empty */
    rp->rio_cnt = read(rp->rio_fd, rp->rio_buf, sizeof(rp->rio_buf));
    if (rp->rio_cnt < 0) {
      if (errno != EINTR) { /* interrupted by sig handler return */
        return -1;
}
    } else if (rp->rio_cnt == 0) { /* EOF */
      return 0;
    } else {
      rp->rio_bufptr = rp->rio_buf; /* reset buffer ptr */
}
  }

  /* Copy min(n, rp->rio_cnt) bytes from internal buf to user buf */
  cnt = n;
  if (rp->rio_cnt < static_cast<int>(n)) { cnt = rp->rio_cnt;
}
  memcpy(usrbuf, rp->rio_bufptr, cnt);
  rp->rio_bufptr += cnt;
  rp->rio_cnt -= cnt;
  return cnt;
}
/* $end rio_read */

/*
 * rio_readinitb - Associate a descriptor with a read buffer and reset buffer
 */
/* $begin rio_readinitb */
void rio_readinitb(rio_t* rp, int fd) {
  rp->rio_fd = fd;
  rp->rio_cnt = 0;
  rp->rio_bufptr = rp->rio_buf;
}
/* $end rio_readinitb */

/*
 * rio_readnb - Robustly read n bytes (buffered)
 */
/* $begin rio_readnb */
ssize_t rio_readnb(rio_t* rp, void* usrbuf, size_t n) {
  size_t nleft = n;
  ssize_t nread;
  char* bufp = static_cast<char*>(usrbuf);

  while (nleft > 0) {
    if ((nread = rio_read(rp, bufp, nleft)) < 0) {
      if (errno == EINTR) { /* interrupted by sig handler return */
        nread = 0;        /* call read() again */
      } else {
        return -1; /* errno set by read() */
}
    } else if (nread == 0) {
      break; /* EOF */
}
    nleft -= nread;
    bufp += nread;
  }
  return (n - nleft); /* return >= 0 */
}
/* $end rio_readnb */

/*
 * rio_readlineb - robustly read a text line (buffered)
 */
/* $begin rio_readlineb */
ssize_t rio_readlineb(rio_t* rp, void* usrbuf, size_t maxlen) {
  size_t n;
  int rc;
  char c;
  char *bufp = static_cast<char*>(usrbuf);

  for (n = 1; n < maxlen; n++) {
    if ((rc = rio_read(rp, &c, 1)) == 1) {
      *bufp++ = c;
      if (c == '\n') { break;
}
    } else if (rc == 0) {
      if (n == 1) {
        return 0; /* EOF, no data read */
      } else {
        break; /* EOF, some data was read */
}
    } else {
      return -1; /* error */
}
  }
  *bufp = 0;
  return n;
}
/* $end rio_readlineb */

/**********************************
 * Wrappers for robust I/O routines
 **********************************/
ssize_t Rio_readn(int fd, void* ptr, size_t nbytes) {
  ssize_t n;

  if ((n = rio_readn(fd, ptr, nbytes)) < 0) {
    LOG(ERROR) << "Rio_readn error: " << strerror(errno);
  }
  return n;
}

void Rio_writen(int fd, void* usrbuf, size_t n) {
  if (rio_writen(fd, usrbuf, n) != static_cast<ssize_t>(n)) {
    LOG(ERROR) << "Rio_writen error: " << strerror(errno);
  }
}

void Rio_readinitb(rio_t* rp, int fd) { rio_readinitb(rp, fd); }

ssize_t Rio_readnb(rio_t* rp, void* usrbuf, size_t n) {
  ssize_t rc;

  if ((rc = rio_readnb(rp, usrbuf, n)) < 0) {
    LOG(ERROR) << "Rio_readnb error: " << strerror(errno);
  }
  return rc;
}

ssize_t Rio_readlineb(rio_t* rp, void* usrbuf, size_t maxlen) {
  ssize_t rc;

  if ((rc = rio_readlineb(rp, usrbuf, maxlen)) < 0) {
    LOG(ERROR) << "Rio_readlineb error: " << strerror(errno);
  }
  return rc;
}

/********************************
 * Client/server helper functions
 ********************************/
/*
 * open_clientfd - open connection to server at <hostname, port>
 *   and return a socket descriptor ready for reading and writing.
 *   Returns -1 and sets errno on Unix error.
 *   Returns -2 and sets h_errno on DNS (gethostbyname) error.
 */
/* $begin open_clientfd */
int open_clientfd(char* hostname, int port) {
  int clientfd;
  struct hostent* hp;
  struct sockaddr_in serveraddr;

  if ((clientfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) { return -1; /* check errno for cause of error */
}

  /* Fill in the server's IP address and port */
  if ((hp = gethostbyname(hostname)) == nullptr) { return -2; /* check h_errno for cause of error */
}
  memset(&serveraddr, 0, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  memmove(&serveraddr.sin_addr.s_addr, hp->h_addr_list[0], hp->h_length);
  serveraddr.sin_port = htons(port);

  /* Establish a connection with the server */
  if (connect(clientfd, reinterpret_cast<SA*>(&serveraddr), sizeof(serveraddr)) < 0) {
    return -1;
  }
  return clientfd;
}
/* $end open_clientfd */

/*
 * open_listenfd - open and return a listening socket on port
 *     Returns -1 and sets errno on Unix error.
 */
/* $begin open_listenfd */
int open_listenfd(int port) {
  int listenfd;
  int optval = 1;
  struct sockaddr_in serveraddr;

  /* Create a socket descriptor */
  if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    return -1;
  }

  /* Eliminates "Address already in use" error from bind. */
  if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int)) < 0) {
    return -1;
  }

  /* Listenfd will be an endpoint for all requests to port
     on any IP address for this host */
  memset(&serveraddr, 0, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serveraddr.sin_port = htons((unsigned short)port);
  if (bind(listenfd, reinterpret_cast<SA*>(&serveraddr), sizeof(serveraddr)) < 0) {
    return -1;
  }

  /* Make it a listening socket ready to accept connection requests */
  if (listen(listenfd, LISTENQ) < 0) { return -1;
}
  return listenfd;
}
/* $end open_listenfd */

/******************************************
 * Wrappers for the client/server helper routines
 ******************************************/
int Open_clientfd(char* hostname, int port) {
  int rc;

  if ((rc = open_clientfd(hostname, port)) < 0) {
    if (rc == -1) {
      LOG(ERROR) << "Open_clientfd Unix error: " << strerror(errno);
    } else {
      LOG(ERROR) << "Open_clientfd DNS error: DNS error " <<  h_errno;
    }
  }
  return rc;
}

int Open_listenfd(int port) {
  int rc;

  if ((rc = open_listenfd(port)) < 0) {
    LOG(ERROR) << "Open_listenfd error: " << strerror(errno);
  }
  return rc;
}
