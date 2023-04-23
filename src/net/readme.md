Pink is a wrapper of pthread. Why you need it?

When you use pthread, you always define many type of thread, such as:

In network programming scenario, some thread used for accept the client's connection, some thread used for pass message to other thread, some thread used for communicate with client using protobuf.

So pink wrap a thin capsulation upon pthread to offer more convinent function.

Model
DispatchThread + Multi WorkerThread



Now pink support these type of thread:

DispatchThread:
DispatchThread is used for listen a port, and get an accept connection. And then dispatch this connection to the worker thread, you can use different protocol, now we have support google protobuf protocol. And you just need write the protocol, and then generate the code. The worker threads will deal with the protocol analysis, so it simplify your job.

Basic usage example:

PikaConnFactory conn_factory;
PikaServerHandle server_handle;
ServerThread *t = new NewDispatchThread(9211, /* server port */
                                        4,    /* worker's number */
                                        &conn_factory,
                                        1000,  /* cron interval */
                                        1000,  /* queue limit */
                                        &server_handle);
t->StartThread();

You can see example/mydispatch_srv.cc for more detail

HolyThread:
HolyThread just like the redis's main thread, it is the thread that both listen a port and do the job. When should you use HolyThread and When should you use DispatchThread combine with WorkerThread, if your job is not so busy, so you can use HolyThread do the all job. if your job is deal lots of client's request, we suggest you use DispathThread with worker threads.

Basic usage example:

PikaConnFactory conn_factory;
PikaServerHandle server_handle;
ServerThread *t = new NewHolyThread(9211, /* server port */
                                    &conn_factory,
                                    1000,  /* cron interval */
                                    &server_handle);
t->StartThread();

You can see example/myholy_srv_chandle.cc example/myholy_srv.cc for more detail

Now we will use pink build our project pika, floyd, zeppelin

In the future, I will add some thread manager in pink.

Dependencies
slash v1.0+

protobuf v2.5.0

OpenSSL if enable ssl

Performance
Test machines:

  --------         --------------
 |        |  <--- | Clients x200 |
 | Server |        --------------
 |        |        --------------
  --------   <--- | Clients x200 |
                   --------------
CPU, Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz, 24 cores

Memory, 142 GB



