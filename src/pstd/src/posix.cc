#include "pstd/include/posix.h"
#include "pstd/include/xdebug.h"

/*********************************************
 * Wrappers for Unix process control functions
 ********************************************/

/* $begin forkwrapper */
pid_t Fork(void) {
  pid_t pid;

  if ((pid = fork()) < 0) {
    log_err("Fork error: %s\n", strerror(errno));
  }
  return pid;
}
/* $end forkwrapper */

void Execve(const char* filename, char* const argv[], char* const envp[]) {
  if (execve(filename, argv, envp) < 0) {
    log_err("Execve error: %s\n", strerror(errno));
  }
}

/* $begin wait */
pid_t Wait(int* status) {
  pid_t pid;

  if ((pid = wait(status)) < 0) {
    log_err("Wait error: %s\n", strerror(errno));
  }
  return pid;
}
/* $end wait */

pid_t Waitpid(pid_t pid, int* iptr, int options) {
  pid_t retpid;

  if ((retpid = waitpid(pid, iptr, options)) < 0) {
    log_err("Waitpid error: %s\n", strerror(errno));
  }
  return (retpid);
}

/* $begin kill */
void Kill(pid_t pid, int signum) {
  int rc;

  if ((rc = kill(pid, signum)) < 0) {
    log_err("Kill error: %s\n", strerror(errno));
  }
}
/* $end kill */

void Pause() {
  (void)pause();
  return;
}

unsigned int Sleep(unsigned int secs) { return sleep(secs); }

unsigned int Alarm(unsigned int seconds) { return alarm(seconds); }

void Setpgid(pid_t pid, pid_t pgid) {
  int rc;

  if ((rc = setpgid(pid, pgid)) < 0) {
    log_err("Setpgid error: %s\n", strerror(errno));
  }
  return;
}

pid_t Getpgrp(void) { return getpgrp(); }

/************************************
 * Wrappers for Unix signal functions
 ***********************************/

/* $begin sigaction */
handler_t* Signal(int signum, handler_t* handler) {
  struct sigaction action, old_action;

  action.sa_handler = handler;
  sigemptyset(&action.sa_mask); /* block sigs of type being handled */
  action.sa_flags = SA_RESTART; /* restart syscalls if possible */

  if (sigaction(signum, &action, &old_action) < 0) {
    log_err("Signal error: %s\n", strerror(errno));
  }
  return (old_action.sa_handler);
}
/* $end sigaction */

void Sigprocmask(int how, const sigset_t* set, sigset_t* oldset) {
  if (sigprocmask(how, set, oldset) < 0) {
    log_err("Sigprocmask error: %s\n", strerror(errno));
  }
  return;
}

void Sigemptyset(sigset_t* set) {
  if (sigemptyset(set) < 0) {
    log_err("Sigemptyset error: %s\n", strerror(errno));
  }
  return;
}

void Sigfillset(sigset_t* set) {
  if (sigfillset(set) < 0) {
    log_err("Sigfillset error: %s\n", strerror(errno));
  }
  return;
}

void Sigaddset(sigset_t* set, int signum) {
  if (sigaddset(set, signum) < 0) {
    log_err("Sigaddset error: %s\n", strerror(errno));
  }
  return;
}

void Sigdelset(sigset_t* set, int signum) {
  if (sigdelset(set, signum) < 0) {
    log_err("Sigdelset error: %s\n", strerror(errno));
  }
  return;
}

int Sigismember(const sigset_t* set, int signum) {
  int rc;
  if ((rc = sigismember(set, signum)) < 0) {
    log_err("Sigismember error: %s\n", strerror(errno));
  }
  return rc;
}

/********************************
 * Wrappers for Unix I/O routines
 ********************************/

int Open(const char* pathname, int flags, mode_t mode) {
  int rc;

  if ((rc = open(pathname, flags, mode)) < 0) {
    log_err("Open error: %s\n", strerror(errno));
  }
  return rc;
}

ssize_t Read(int fd, void* buf, size_t count) {
  ssize_t rc;

  if ((rc = read(fd, buf, count)) < 0) {
    log_err("Read error: %s\n", strerror(errno));
  }
  return rc;
}

ssize_t Write(int fd, const void* buf, size_t count) {
  ssize_t rc;

  if ((rc = write(fd, buf, count)) < 0) {
    log_err("Write error: %s\n", strerror(errno));
  }
  return rc;
}

off_t Lseek(int fildes, off_t offset, int whence) {
  off_t rc;

  if ((rc = lseek(fildes, offset, whence)) < 0) {
    log_err("Lseek error: %s\n", strerror(errno));
  }
  return rc;
}

void Close(int fd) {
  int rc;

  if ((rc = close(fd)) < 0) {
    log_err("Close error: %s\n", strerror(errno));
  }
}

int Select(int n, fd_set* readfds, fd_set* writefds, fd_set* exceptfds, struct timeval* timeout) {
  int rc;

  if ((rc = select(n, readfds, writefds, exceptfds, timeout)) < 0) {
    log_err("Select error: %s\n", strerror(errno));
  }
  return rc;
}

int Dup2(int fd1, int fd2) {
  int rc;

  if ((rc = dup2(fd1, fd2)) < 0) {
    log_err("Dup2 error: %s\n", strerror(errno));
  }
  return rc;
}

void Stat(const char* filename, struct stat* buf) {
  if (stat(filename, buf) < 0) {
    log_err("Stat error: %s\n", strerror(errno));
  }
}

void Fstat(int fd, struct stat* buf) {
  if (fstat(fd, buf) < 0) {
    log_err("Fstat error: %s\n", strerror(errno));
  }
}

/***************************************
 * Wrappers for memory mapping functions
 ***************************************/
void* Mmap(void* addr, size_t len, int prot, int flags, int fd, off_t offset) {
  void* ptr;

  if ((ptr = mmap(addr, len, prot, flags, fd, offset)) == ((void*)-1)) {
    log_err("mmap error: %s\n", strerror(errno));
  }
  return (ptr);
}

void Munmap(void* start, size_t length) {
  if (munmap(start, length) < 0) {
    log_err("munmap error: %s\n", strerror(errno));
  }
}

/***************************************************
 * Wrappers for dynamic storage allocation functions
 ***************************************************/

void* Malloc(size_t size) {
  void* p;

  if ((p = malloc(size)) == NULL) {
    log_err("Malloc error: %s\n", strerror(errno));
  }
  return p;
}

void* Realloc(void* ptr, size_t size) {
  void* p;

  if ((p = realloc(ptr, size)) == NULL) {
    log_err("Realloc error: %s\n", strerror(errno));
  }
  return p;
}

void* Calloc(size_t nmemb, size_t size) {
  void* p;

  if ((p = calloc(nmemb, size)) == NULL) {
    log_err("Calloc error: %s\n", strerror(errno));
  }
  return p;
}

void Free(void* ptr) { free(ptr); }

/******************************************
 * Wrappers for the Standard I/O functions.
 ******************************************/
void Fclose(FILE* fp) {
  if (fclose(fp) != 0) {
    log_err("Fclose error: %s\n", strerror(errno));
  }
}

FILE* Fdopen(int fd, const char* type) {
  FILE* fp;

  if ((fp = fdopen(fd, type)) == NULL) {
    log_err("Fdopen error: %s\n", strerror(errno));
  }

  return fp;
}

char* Fgets(char* ptr, int n, FILE* stream) {
  char* rptr;

  if (((rptr = fgets(ptr, n, stream)) == NULL) && ferror(stream)) {
    log_err("Fgets error");
  }

  return rptr;
}

FILE* Fopen(const char* filename, const char* mode) {
  FILE* fp;

  if ((fp = fopen(filename, mode)) == NULL) {
    log_err("Fopen error: %s\n", strerror(errno));
  }

  return fp;
}

void Fputs(const char* ptr, FILE* stream) {
  if (fputs(ptr, stream) == EOF) {
    log_err("Fputs error: %s\n", strerror(errno));
  }
}

size_t Fread(void* ptr, size_t size, size_t nmemb, FILE* stream) {
  size_t n;

  if (((n = fread(ptr, size, nmemb, stream)) < nmemb) && ferror(stream)) {
    log_err("Fread error: %s\n", strerror(errno));
  }
  return n;
}

void Fwrite(const void* ptr, size_t size, size_t nmemb, FILE* stream) {
  if (fwrite(ptr, size, nmemb, stream) < nmemb) {
    log_err("Fwrite error: %s\n", strerror(errno));
  }
}

/****************************
 * Sockets interface wrappers
 ****************************/

int Socket(int domain, int type, int protocol) {
  int rc;

  if ((rc = socket(domain, type, protocol)) < 0) {
    log_err("Socket error: %s\n", strerror(errno));
  }
  return rc;
}

void Setsockopt(int s, int level, int optname, const void* optval, int optlen) {
  int rc;

  if ((rc = setsockopt(s, level, optname, optval, optlen)) < 0) {
    log_err("Setsockopt error: %s\n", strerror(errno));
  }
}

void Bind(int sockfd, struct sockaddr* my_addr, int addrlen) {
  int rc;

  if ((rc = bind(sockfd, my_addr, addrlen)) < 0) {
    log_err("Bind error: %s\n", strerror(errno));
  }
}

void Listen(int s, int backlog) {
  int rc;

  if ((rc = listen(s, backlog)) < 0) {
    log_err("Listen error: %s\n", strerror(errno));
  }
}

int Accept(int s, struct sockaddr* addr, socklen_t* addrlen) {
  int rc;

  if ((rc = accept(s, addr, addrlen)) < 0) {
    log_err("Accept error: %s\n", strerror(errno));
  }
  return rc;
}

void Connect(int sockfd, struct sockaddr* serv_addr, int addrlen) {
  int rc;

  if ((rc = connect(sockfd, serv_addr, addrlen)) < 0) {
    log_err("Connect error: %s\n", strerror(errno));
  }
}

/************************
 * DNS interface wrappers
 ***********************/

/* $begin gethostbyname */
struct hostent* Gethostbyname(const char* name) {
  struct hostent* p;

  if ((p = gethostbyname(name)) == NULL) {
    log_err("%s: DNS error %d\n", "Gethostbyname error", h_errno);
  }
  return p;
}
/* $end gethostbyname */

struct hostent* Gethostbyaddr(const char* addr, int len, int type) {
  struct hostent* p;

  if ((p = gethostbyaddr(addr, len, type)) == NULL) {
    log_err("%s: DNS error %d\n", "Gethostbyaddr error", h_errno);
  }
  return p;
}

/************************************************
 * Wrappers for Pthreads thread control functions
 ************************************************/

void Pthread_create(pthread_t* tidp, pthread_attr_t* attrp, void* (*routine)(void*), void* argp) {
  int rc;

  if ((rc = pthread_create(tidp, attrp, routine, argp)) != 0) {
    log_err("Pthread_create error: %s\n", strerror(rc));
  }
}

void Pthread_cancel(pthread_t tid) {
  int rc;

  if ((rc = pthread_cancel(tid)) != 0) {
    log_err("Pthread_cancel error: %s\n", strerror(rc));
  }
}

void Pthread_join(pthread_t tid, void** thread_return) {
  int rc;

  if ((rc = pthread_join(tid, thread_return)) != 0) {
    log_err("Pthread_join error: %s\n", strerror(rc));
  }
}

/* $begin detach */
void Pthread_detach(pthread_t tid) {
  int rc;

  if ((rc = pthread_detach(tid)) != 0) {
    log_err("Pthread_detach error: %s\n", strerror(rc));
  }
}
/* $end detach */

void Pthread_exit(void* retval) { pthread_exit(retval); }

pthread_t Pthread_self(void) { return pthread_self(); }

void Pthread_once(pthread_once_t* once_control, void (*init_function)()) { pthread_once(once_control, init_function); }

/*******************************
 * Wrappers for Posix semaphores
 *******************************/

void Sem_init(sem_t* sem, int pshared, unsigned int value) {
  if (sem_init(sem, pshared, value) < 0) {
    log_err("Sem_init error: %s\n", strerror(errno));
  }
}

void P(sem_t* sem) {
  if (sem_wait(sem) < 0) {
    log_err("P error: %s\n", strerror(errno));
  }
}

void V(sem_t* sem) {
  if (sem_post(sem) < 0) {
    log_err("V error: %s\n", strerror(errno));
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
  char* bufp = (char*)usrbuf;

  while (nleft > 0) {
    if ((nread = read(fd, bufp, nleft)) < 0) {
      if (errno == EINTR) /* interrupted by sig handler return */
        nread = 0;        /* and call read() again */
      else
        return -1; /* errno set by read() */
    } else if (nread == 0)
      break; /* EOF */
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
  char* bufp = (char*)usrbuf;

  while (nleft > 0) {
    if ((nwritten = write(fd, bufp, nleft)) <= 0) {
      if (errno == EINTR) /* interrupted by sig handler return */
        nwritten = 0;     /* and call write() again */
      else
        return -1; /* errorno set by write() */
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
      if (errno != EINTR) /* interrupted by sig handler return */
        return -1;
    } else if (rp->rio_cnt == 0) /* EOF */
      return 0;
    else
      rp->rio_bufptr = rp->rio_buf; /* reset buffer ptr */
  }

  /* Copy min(n, rp->rio_cnt) bytes from internal buf to user buf */
  cnt = n;
  if (rp->rio_cnt < (int)n) cnt = rp->rio_cnt;
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
  char* bufp = (char*)usrbuf;

  while (nleft > 0) {
    if ((nread = rio_read(rp, bufp, nleft)) < 0) {
      if (errno == EINTR) /* interrupted by sig handler return */
        nread = 0;        /* call read() again */
      else
        return -1; /* errno set by read() */
    } else if (nread == 0)
      break; /* EOF */
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
  char c, *bufp = (char*)usrbuf;

  for (n = 1; n < maxlen; n++) {
    if ((rc = rio_read(rp, &c, 1)) == 1) {
      *bufp++ = c;
      if (c == '\n') break;
    } else if (rc == 0) {
      if (n == 1)
        return 0; /* EOF, no data read */
      else
        break; /* EOF, some data was read */
    } else
      return -1; /* error */
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
    log_err("Rio_readn error: %s\n", strerror(errno));
  }
  return n;
}

void Rio_writen(int fd, void* usrbuf, size_t n) {
  if (rio_writen(fd, usrbuf, n) != (ssize_t)n) {
    log_err("Rio_writen error: %s\n", strerror(errno));
  }
}

void Rio_readinitb(rio_t* rp, int fd) { rio_readinitb(rp, fd); }

ssize_t Rio_readnb(rio_t* rp, void* usrbuf, size_t n) {
  ssize_t rc;

  if ((rc = rio_readnb(rp, usrbuf, n)) < 0) {
    log_err("Rio_readnb error: %s\n", strerror(errno));
  }
  return rc;
}

ssize_t Rio_readlineb(rio_t* rp, void* usrbuf, size_t maxlen) {
  ssize_t rc;

  if ((rc = rio_readlineb(rp, usrbuf, maxlen)) < 0) {
    log_err("Rio_readlineb error: %s\n", strerror(errno));
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

  if ((clientfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) return -1; /* check errno for cause of error */

  /* Fill in the server's IP address and port */
  if ((hp = gethostbyname(hostname)) == NULL) return -2; /* check h_errno for cause of error */
  bzero((char*)&serveraddr, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  bcopy((char*)hp->h_addr_list[0], (char*)&serveraddr.sin_addr.s_addr, hp->h_length);
  serveraddr.sin_port = htons(port);

  /* Establish a connection with the server */
  if (connect(clientfd, (SA*)&serveraddr, sizeof(serveraddr)) < 0) return -1;
  return clientfd;
}
/* $end open_clientfd */

/*
 * open_listenfd - open and return a listening socket on port
 *     Returns -1 and sets errno on Unix error.
 */
/* $begin open_listenfd */
int open_listenfd(int port) {
  int listenfd, optval = 1;
  struct sockaddr_in serveraddr;

  /* Create a socket descriptor */
  if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) return -1;

  /* Eliminates "Address already in use" error from bind. */
  if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (const void*)&optval, sizeof(int)) < 0) return -1;

  /* Listenfd will be an endpoint for all requests to port
     on any IP address for this host */
  bzero((char*)&serveraddr, sizeof(serveraddr));
  serveraddr.sin_family = AF_INET;
  serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
  serveraddr.sin_port = htons((unsigned short)port);
  if (bind(listenfd, (SA*)&serveraddr, sizeof(serveraddr)) < 0) return -1;

  /* Make it a listening socket ready to accept connection requests */
  if (listen(listenfd, LISTENQ) < 0) return -1;
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
      log_err("Open_clientfd Unix error: %s\n", strerror(errno));
    } else {
      log_err("%s: DNS error %d\n", "Open_clientfd DNS error", h_errno);
    }
  }
  return rc;
}

int Open_listenfd(int port) {
  int rc;

  if ((rc = open_listenfd(port)) < 0) {
    log_err("Open_listenfd error: %s\n", strerror(errno));
  }
  return rc;
}
