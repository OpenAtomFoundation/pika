#include <iostream>
#include "scan.h"
#include "insert.h"

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "    ./txt_to_pika txt pika_ip pika_port [ttl]" << std::endl;
  std::cout << "    example: ./txt_to_pika data.txt 127.0.0.1 9921 10" << std::endl;
}

int main(int argc, char **argv) {
  if (argc < 4 || argc > 5) {
    Usage();
    return 0;
  }

  std::string filename = std::string(argv[1]);
  std::string ip = std::string(argv[2]);
  int port = std::stoi(std::string(argv[3]));
  int ttl = 0;

  if (argc == 5) {
    ttl = std::stoi(std::string(argv[4]));
  }

  std::cout << "filename: " << filename << std::endl;
  std::cout << "ip: " << ip << std::endl;
  std::cout << "port: " << port << std::endl;
  std::cout << "ttl: " << ttl << std::endl;

  ScanThread* scan_thread = new ScanThread(filename);
  scan_thread->StartThread();

  scan_thread->JoinThread();

  //std::cout <<"Total " << scan_thread->Num()  << " records has been scaned"<< std::endl;
  //std::cout <<"Total " << write_thread->Num() << " records hash been inserted to file" << std::endl;

  delete scan_thread;

  return 0; 
}

