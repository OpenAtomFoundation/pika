#include <chrono>
#include <iostream>
#include "scan.h"
#include "sender.h"

using std::chrono::high_resolution_clock;

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "    ./txt_to_pika txt pika_ip pika_port -n [thread_num] -t [ttl] -p [password]" << std::endl;
  std::cout << "    example: ./txt_to_pika data.txt 127.0.0.1 9921 -n 10 -t 10 -p 123456" << std::endl;
}

int main(int argc, char** argv) {
  if (argc < 4) {
    Usage();
    return 0;
  }

  high_resolution_clock::time_point start = high_resolution_clock::now();

  std::string password;
  int thread_num = 8;
  std::string filename = std::string(argv[1]);
  std::string ip = std::string(argv[2]);
  int port = std::stoi(std::string(argv[3]));
  int ttl = -1;
  std::vector<SenderThread*> senders;

  int index = 4;
  if (argc > 4) {
    while (index < (argc - 1)) {
      int flag = index++;
      if (std::string(argv[flag]) == "-n") {
        thread_num = std::stoi(std::string(argv[index++]));
        if (index > (argc - 1)) {
          break;
        }
      }
      if (std::string(argv[flag]) == "-t") {
        ttl = std::stoi(std::string(argv[index++]));
        if (index > (argc - 1)) {
          break;
        }
      }
      if (std::string(argv[flag]) == "-p") {
        password = std::string(argv[index++]);
        if (index > (argc - 1)) {
          break;
        }
      }
    }
  }

  std::cout << "filename: " << filename << std::endl;
  std::cout << "ip: " << ip << std::endl;
  std::cout << "port: " << port << std::endl;
  std::cout << "ttl: " << ttl << std::endl;
  std::cout << "thread: " << thread_num << std::endl;
  std::cout << "password: " << password << std::endl;

  for (int i = 0; i < thread_num; i++) {
    senders.push_back(new SenderThread(ip, port, password));
  }

  ScanThread* scan_thread = new ScanThread(filename, senders, ttl);

  for (int i = 0; i < thread_num; i++) {
    senders[i]->StartThread();
  }

  scan_thread->StartThread();
  scan_thread->JoinThread();

  for (int i = 0; i < thread_num; i++) {
    senders[i]->Stop();
  }

  for (int i = 0; i < thread_num; i++) {
    senders[i]->JoinThread();
  }

  int records = 0;
  for (int i = 0; i < thread_num; i++) {
    records += senders[i]->elements();
  }

  for (int i = 0; i < thread_num; i++) {
    delete senders[i];
  }

  std::cout << std::endl << "Total " << scan_thread->Num() << " records has been scaned" << std::endl;
  std::cout << "Total " << records << " records hash been executed by pika" << std::endl;

  delete scan_thread;

  high_resolution_clock::time_point end = high_resolution_clock::now();
  std::chrono::hours h = std::chrono::duration_cast<std::chrono::hours>(end - start);
  std::chrono::minutes m = std::chrono::duration_cast<std::chrono::minutes>(end - start);
  std::chrono::seconds s = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  std::cout << "====================================" << std::endl;
  std::cout << "Running time  :";
  std::cout << h.count() << " hour " << m.count() - h.count() * 60 << " min " << s.count() - h.count() * 60 * 60
            << " s\n"
            << std::endl;
  return 0;
}
