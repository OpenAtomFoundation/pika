#include <iostream>
#include <chrono>

#include "scan.h"
#include "write.h"

using std::chrono::high_resolution_clock;

void Usage() {
  std::cout << "Usage: " << std::endl;
  std::cout << "    ./pika_to_txt db_path [filename]" << std::endl;
  std::cout << "    example: ./pika_to_txt ~/db data.txt" << std::endl;
}

int main(int argc, char **argv) {
  if (argc < 2 || argc > 3) {
    Usage();
    return 0;
  }

  high_resolution_clock::time_point start = high_resolution_clock::now();

  std::string db_path = std::string(argv[1]);
  std::string filename = "data.txt";
  if (argc == 3) {
    filename = argv[2]; 
  }

  if (db_path[db_path.length() - 1] != '/') {
    db_path.append("/");
  }
  std::cout << db_path << std::endl;

  // Init db
  nemo::Options option;
  option.write_buffer_size = 256 * 1024 * 1024; // 256M
  option.target_file_size_base = 20 * 1024 * 1024; // 20M
  nemo::Nemo *db = new nemo::Nemo(db_path, option);

  // Init scan thread
  WriteThread* write_thread = new WriteThread(filename);
  ScanThread* scan_thread = new ScanThread(db, write_thread);

  scan_thread->StartThread();
  write_thread->StartThread();
  
  scan_thread->JoinThread();
  write_thread->JoinThread();

  std::cout <<"Total " << scan_thread->Num()  << " records has been scaned"<< std::endl;
  std::cout <<"Total " << write_thread->Num() << " records hash been writed to file" << std::endl;
  delete db;
  delete write_thread;
  delete scan_thread;

  high_resolution_clock::time_point end = high_resolution_clock::now();
  std::chrono::hours  h = std::chrono::duration_cast<std::chrono::hours>(end - start);
  std::chrono::minutes  m = std::chrono::duration_cast<std::chrono::minutes>(end - start);
  std::chrono::seconds  s = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  std::cout << "====================================" << std::endl;
  std::cout << "Running time  :";
  std::cout << h.count() << " hour " << m.count() - h.count() * 60 << " min " << s.count() - h.count() * 60 * 60 << " s\n" << std::endl;

  return 0; 
}

