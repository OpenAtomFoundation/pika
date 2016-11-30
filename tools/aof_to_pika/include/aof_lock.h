#ifndef AOF_LOCK_H
#define AOF_LOCK_H

class CondVar;

class Mutex {
    public:
        Mutex();
        ~Mutex();

        void Lock();
        void Unlock();
        void AssertHeld() { }

    private:
        friend class CondVar;
        pthread_mutex_t mu_;

        // No copying
        Mutex(const Mutex&);
        void operator=(const Mutex&);
};

class CondVar {
    public:
        explicit CondVar(Mutex* mu);
        ~CondVar();
        void Wait();
        void Signal();
        void SignalAll();
    private:
        pthread_cond_t cv_;
        Mutex* mu_;
};

#endif  // AOF_LOCK_H
