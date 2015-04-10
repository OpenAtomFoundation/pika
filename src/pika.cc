#include "pika.h"
#include "pika_worker.h"
#include "pika_hb.h"
#include "pika_meta.h"
#include "pika_hb_monitor.h"

Pika::Pika()
{
	pikaMeta_ = new PikaMeta();
	pikaWorker_ = new PikaWorker();
	pikaHb_ = new PikaHb(&preNode_);
	pikaHbMonitor_ = new PikaHbMonitor(&preNode_);
}

Pika::~Pika()
{
	delete pikaHbMonitor_;
	delete pikaHb_;
	delete pikaWorker_;
	delete pikaMeta_;
}

int Pika::RunHb()
{
	PikaHb::CreateHb(pikaHb_->thread_id(), pikaHb_);
	return 0;
}

int Pika::RunHbMonitor()
{
	PikaHbMonitor::CreateHbMonitor(pikaHbMonitor_->thread_id(), pikaHbMonitor_);
	return 0;
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

int Pika::RunPulse()
{
	pthread_create(&hbId_, NULL, &(Pika::StartPulse), pikaHb_);
}

void* Pika::StartPulse(void* arg)
{
	reinterpret_cast<PikaHb*>(arg)->StartPulse();
	return NULL;
}
