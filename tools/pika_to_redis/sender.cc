#include "sender.h"

Sender::Sender(std::string ip, int64_t port, std::string password):
	ip_(ip),
	port_(port),
	password_(password),
    should_exit_(false),
    elements_(0),
    full_(10000)
    {
    }
	    
Sender::~Sender() {
  //delete bg_thread_;
}

/*
void Sender::SendData(void *arg) {
  //std::cout << " task : " << *((int *)arg) << std::endl;
  //delete (int*)arg;
  //struct DataPack *data = ((DataPack*)arg);
  //std::cout << data->cmd << std::endl;

  //std::string send_data = std::string(data->cmd);
  
  std::cout << "send ..." << std::endl;
  
  slash::Status s = data->cli->Send(&send_data);

  if (!s.ok()) {
	// TODO log  and retry      	
    data->cli->Close();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  } else {
	//std::cout << "send..." << std::endl;
	//delete (DataPack*)arg;
  }
 
*/

void Sender::LoadCmd(const std::string &cmd) {
	if (QueueSize() <= full_) {
	  slash::MutexLock l(&cmd_mutex_);
	  cmd_queue_.push(cmd);
	} else {
	  log_info("The maximum length of a queue is more than %d, wait for 1 seconds.", int(full_));
	  std::this_thread::sleep_for(std::chrono::microseconds(1));
	}
	//struct DataPack *data = new DataPack;
	//data->cli = cli_;
	//data->cmd = cmd;
	//bg_thread_->Schedule(SendData, NULL);
    //bg_thread_->Schedule(SendData, (void*)data);
    //elements_++;
	//bg_thread_->Schedule(&SendData, (void*)&data);
}

void *Sender::ThreadMain() {
  log_info("Start sender thread...");

  pink::PinkCli *cli = NULL;
  
  while (!should_exit_ || QueueSize() != 0) {
  	if (QueueSize() == 0)
  		continue;
  	if (cli == NULL) {
  	  // Connect to redis
      cli = pink::NewRedisCli();
  	  cli->set_connect_timeout(1000);
  	  slash::Status s = cli->Connect(ip_, port_);
  	  if (!s.ok()) {
  	  	cli = NULL;
    	log_info("Can not connect to %s:%d: %s", ip_.data(), port_, s.ToString().data());
	  	continue;
	  } else {
	  	// Connect success
	  	log_info("Connect to %s:%d:%s", ip_.data(), port_, s.ToString().data());
	  	
	  	// Authentication
  		if (!password_.empty()) {
    	  pink::RedisCmdArgsType argv, resp;
    	  std::string cmd;

    	  argv.push_back("AUTH");
    	  argv.push_back(password_);
    	  pink::SerializeRedisCommand(argv, &cmd);
    	  slash::Status s = cli->Send(&cmd);
    	  
    	  if (s.ok()) {
      	  	s = cli->Recv(&resp);
       	  	if (resp[0] == "OK") {
  	  		  log_info("Authentic success");
  	  	    } else {
  	  		  log_info("Invalid password");
  	  		  return NULL;
  	  		}
  		  } else {
  		  	cli->Close();
  	  	    log_info("%s", s.ToString().data());
  	  	    cli = NULL;
  	 		continue;
    	  }
  		} else {
  		  // If forget to input password
  		  pink::RedisCmdArgsType argv, resp;
    	  std::string cmd;

    	  argv.push_back("PING");
    	  pink::SerializeRedisCommand(argv, &cmd);
    	  slash::Status s = cli->Send(&cmd);
    	  
    	  if (s.ok()) {
      	  	s = cli->Recv(&resp);
      	  	if (s.ok()) {
       	  	  if (resp[0] == "NOAUTH Authentication required.") {
  	  		  log_info("Authentication required");
  	  		  return NULL;
  	  	      }
  	  	    } else {
  	  	      cli->Close();
  	  	      log_info("%s", s.ToString().data());
  	  	      cli = NULL;
  	 		  continue;
  	 		}
  	 	  }
  		}
	  }
  	} else {
	  slash::MutexLock l(&cmd_mutex_);
  	  slash::Status s = cli->Send(&cmd_queue_.front());
  	  if (s.ok()) {
  	    elements_++;
  	    cmd_queue_.pop();
  	  } else {
  	    cli->Close();
  	    log_info("%s", s.ToString().data());
  	    cli = NULL;
        std::this_thread::sleep_for(std::chrono::microseconds(1));
  	  }
    }
  }
  delete cli;
  log_info("Sender thread complete");
  return NULL;
}