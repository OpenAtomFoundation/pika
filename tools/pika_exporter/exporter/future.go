package exporter

import "sync"

type futureKey struct {
	addr, alias string
}

type future struct {
	*sync.Mutex
	wait sync.WaitGroup
	m    map[futureKey]error
}

func newFuture() *future {
	return &future{
		Mutex: new(sync.Mutex),
		m:     make(map[futureKey]error),
	}
}

func (f *future) Add() {
	f.wait.Add(1)
}

func (f *future) Done(key futureKey, val error) {
	f.Lock()
	defer f.Unlock()
	f.m[key] = val
	f.wait.Done()
}

func (f *future) Wait() map[futureKey]error {
	f.wait.Wait()
	f.Lock()
	defer f.Unlock()
	return f.m
}

type futureKeyForProxy struct {
	addr, instance, ID, productName string
}

type futureForProxy struct {
	*sync.Mutex
	wait sync.WaitGroup
	m    map[futureKeyForProxy]error
}

func newFutureForProxy() *futureForProxy {
	return &futureForProxy{
		Mutex: new(sync.Mutex),
		m:     make(map[futureKeyForProxy]error),
	}
}

func (f *futureForProxy) Add() {
	f.wait.Add(1)
}

func (f *futureForProxy) Done(key futureKeyForProxy, val error) {
	f.Lock()
	defer f.Unlock()
	f.m[key] = val
	f.wait.Done()
}

func (f *futureForProxy) Wait() map[futureKeyForProxy]error {
	f.wait.Wait()
	f.Lock()
	defer f.Unlock()
	return f.m
}
