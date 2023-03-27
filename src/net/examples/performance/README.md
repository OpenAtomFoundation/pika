client and server code used to get net performance benchmark

### usage

after compiler you will get two executable program server and client

start server
./server 127.0.0.1(you ip) port(listen port)

./client 127.0.0.1(server ip) port(sever port)

since there should be many clients to get the net's performance limitation,
so in our case, we will always have 10~20 client to pressure measure server
