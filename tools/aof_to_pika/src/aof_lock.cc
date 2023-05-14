#include <pthread.h>
#include <cstdlib>
#include <iostream>
#include <string>

#include "aof_lock.h"

static void PthreadCall(const std::string& label, int result) {
  if (result != 0) {
    std::cout << "pthread " << label << " : " << result << std::endl;
    abort();
  }
}

Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr)); }

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }

void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

CondVar::CondVar(Mutex* mu) : mu_(mu) { PthreadCall("init cv", pthread_cond_init(&cv_, nullptr)); }

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() { PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_)); }

void CondVar::Signal() { PthreadCall("signal", pthread_cond_signal(&cv_)); }

void CondVar::SignalAll() { PthreadCall("broadcast", pthread_cond_broadcast(&cv_)); }
