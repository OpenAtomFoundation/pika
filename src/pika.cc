#include "pika.h"
#include "pika_worker.h"
#include "pika_hb.h"

Pika::Pika()
{
	pikaWorker_ = new PikaWorker();
	pikaHb_ = new PikaHb();
}

Pika::~Pika()
{
	delete pikaHb_;
	delete pikaWorker_;
}

int Pika::RunHb()
{
	pthread_create(&hbId_, NULL, &(Pika::StartHb), pikaHb_);
	return 0;
}

void* Pika::StartHb(void* arg)
{
	reinterpret_cast<PikaHb*>(arg)->Start();
	return NULL;
}

int Pika::RunWorker()
{
	pikaWorker_->Start();
	/*
	 * pthread_create(&workerId_, NULL, &(Pika::StartHb), pikaWorker_);
	 */
	return 0;
}

void* Pika::StartWorker(void* arg)
{
	reinterpret_cast<PikaWorker*>(arg)->Start();
	return NULL;
}

