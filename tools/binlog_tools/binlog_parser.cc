
#include <glog/logging.h>
#include <iostream>
#include "binlog.h"
#include "binlog_consumer.h"
#include "binlog_producer.h"

#define INPUT_FILESIZE 104857600
enum ConvertType {
  old2new = 1,
  new2old = 2,
  new2read = 3
};

static void Usage()
{
    fprintf(stderr,
            "Usage: binlogparser [-h] [-c old2new -i input_path -o output_path -f filenumber -t offset ]\n"
            "\tBinlog_parser converts between pika's old binlog(before 2.1.0) and pika's new version binlog in timestamped format\n"
            "\t-h     -- show this help\n"
            "\t-c     -- convert case: old2new new2old new2read,default: old2new\n"
            "\t-i     -- path of input binlog files , default: ./old_log/\n"
            "\t-o     -- path to store output binlog files , default: ./new_log/ \n"
            "\t-f     -- files to convert , seperated by , default: 0\n"
            "  example: ./binlog_parser -c old2new -i old_binlog_path -o new_binlog_path -f 0,1,2,3 \n"
           );
}
  
  
int GetFiles(std::string& files_str, std::string& pattern, std::vector<int>& files) {
  std::string::size_type pos;  
  std::string file;  
  files_str += pattern;
  int str_size = files_str.size();  
  for(int i = 0; i < str_size; i++) {
    pos = files_str.find(pattern,i);  
    if (pos != (unsigned int)str_size) {
      file = files_str.substr(i, pos - i);  
      files.push_back(atoi(file.c_str()));  
      i = pos + pattern.size() - 1;  
    }  
  }  
  return files.size();  
}

bool CheckSequential(std::vector<int>& seq) {
  bool isSeq = true;
  if (seq.size() <= 1)
    return isSeq;
  for (unsigned int i = 0; i <= seq.size() - 2; i++) {
    if (seq[i+1] != seq[i] + 1) {
      isSeq = false;
      break;
    }
  }
  return isSeq;
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  std::string input_path = "./old_log/";
  std::string output_path = "./new_log/";
  std::string convert_type_str = "old2new";
  std::string files_str = "0";

  // for correct inputs , we use these flags to generate warning to user
  bool default_input_path = true;
  bool default_output_path = true;
  bool default_convert_type = true;
  bool default_files = true;
  char c;
  while (-1 != (c = getopt(argc, argv, "hi:c:o:f:t:"))) {
    switch (c) {
      case 'h':
        Usage();
        exit(-1);
      case 'i':
        input_path = optarg;
        if (input_path[input_path.length() - 1] != '/' ) {
          input_path.append("/");
        }
        default_input_path = false;
        break;
      case 'c':
        convert_type_str = optarg;
        default_convert_type = false;
        break;
      case 'o':
        output_path = optarg;
        if (output_path[output_path.length() - 1] != '/' ) {
          output_path.append("/");
        }
        default_output_path = false;
        break;
      case 'f':
        files_str = optarg;
        default_files = false;
        break;
      default:
        Usage();
        exit(-1);
        return 0;
    }
  }

  if (input_path == output_path) {
    fprintf (stderr, "Error: conflict path for input and output path \n" );
    exit(-1);
  }
  if (default_input_path) {
    fprintf (stderr, "Warning: use default input file path\n" );
  }
  if (default_output_path) {
    fprintf (stderr, "Warning: use default input file path\n" );
  }
  if (default_files) {
    fprintf (stderr, "Warning: use default input file number\n" );
  }
  if (default_convert_type) {
    fprintf (stderr, "Warning: use default convert type:%s \n", convert_type_str.c_str());
  }

  ConvertType convert_type = old2new;
  if (convert_type_str == "old2new") {
    convert_type = old2new;
  } else if (convert_type_str == "new2old") {
    convert_type = new2old;
  } else if (convert_type_str == "new2read") {
    convert_type = new2read;
  } else {
    fprintf (stderr, "undefined convert case: old2new or new2old new2read only\n" );
    exit(-1);
  }

  std::vector<int> files;
  std::string comma = std::string(",");
  int file_num = GetFiles(files_str, comma, files);
  int start_file = files[0];
  bool isSequential = CheckSequential(files);
  if (!isSequential) {
      std::cout << "please input sequential binlog num :" << std::endl;
      exit(-1);
  }

  Binlog* old_logger = new Binlog(input_path, INPUT_FILESIZE);
  BinlogConsumer* binlog_consumer;
  BinlogProducer* binlog_producer;
  if (convert_type == old2new) {
    binlog_consumer = new OldBinlogConsumer(old_logger);
    binlog_producer = new NewBinlogProducer(output_path);
  } else if(convert_type == new2old) {
    binlog_consumer = new NewBinlogConsumer(old_logger);
    binlog_producer = new OldBinlogProducer(output_path);
  } else if(convert_type == new2read) {
    binlog_consumer = new NewBinlogConsumer(old_logger);
    binlog_producer = new ReadableBinlogProducer(output_path);
  }
 




  Status s;
  s = binlog_consumer->LoadFile(start_file);
  if(!s.ok()) {
      std::cout << "something wrong while loading binlog:" << s.ToString() << std::endl;
      exit(-1);
  }
  s = binlog_producer->LoadFile(start_file);
  std::string scratch;
  scratch.reserve(1024 * 1024);
  int finished_num = 0;
  uint64_t produce_time;
  ReadableBinlogProducer* readable_producer_proxy = dynamic_cast<ReadableBinlogProducer *>(binlog_producer);
  while (true){
    s = binlog_consumer->Parse(scratch, &produce_time);
    if (s.IsEndFile()) {
      std::cout << "parse binlog file:"<< NewFileName(old_logger->filename, start_file) << " finished" << std::endl;
      finished_num ++;
      if (finished_num < file_num) {
        s = binlog_consumer->LoadNextFile();
        s = binlog_producer->LoadNextFile();
      } else {
        break;
      }
    } else if (s.IsComplete()) {
      std::cout << "all binlog parsed" << std::endl;
      break;
    } else if (s.ok()) {
      if (convert_type == new2read) {
        readable_producer_proxy->Put(scratch, produce_time);
      } else {
        binlog_producer->Put(scratch);
      }
    } else if (!s.ok()) {
      std::cout << "something wrong when parsing old binlog " << std::endl;
      break;
    }
  }


  
  std::cout << "testing new binlog.............................................. " << std::endl;
  Binlog* new_logger = new Binlog(output_path, INPUT_FILESIZE);
  std::cout << "test result:new binlog can be loaded " << std::endl;
  delete binlog_consumer;
  delete binlog_producer;
  delete old_logger;
  delete new_logger;
  return 0;
}
