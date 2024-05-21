// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"sync"
	"time"

	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/proxy"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/redis"
	"pika/codis/v2/pkg/utils/rpc"
	"pika/codis/v2/pkg/utils/sync2"
)

type RedisStats struct {
	Stats map[string]string `json:"stats,omitempty"`
	Error *rpc.RemoteError  `json:"error,omitempty"`

	Sentinel map[string]*redis.SentinelGroup `json:"sentinel,omitempty"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) newRedisStats(addr string, timeout time.Duration, do func(addr string) (*RedisStats, error)) *RedisStats {
	var ch = make(chan struct{})
	stats := &RedisStats{}

	go func() {
		defer close(ch)
		p, err := do(addr)
		if err != nil {
			stats.Error = rpc.NewRemoteError(err)
		} else {
			stats.Stats, stats.Sentinel = p.Stats, p.Sentinel
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return &RedisStats{Timeout: true}
	}
}

func (s *Topom) RefreshRedisStats(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	goStats := func(addr string, do func(addr string) (*RedisStats, error)) {
		fut.Add()
		go func() {
			stats := s.newRedisStats(addr, timeout, do)
			stats.UnixTime = time.Now().Unix()
			fut.Done(addr, stats)
		}()
	}
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			goStats(x.Addr, func(addr string) (*RedisStats, error) {
				m, err := s.stats.redisp.InfoFullv2(addr)
				if err != nil {
					return nil, err
				}
				return &RedisStats{Stats: m}, nil
			})
		}
	}
	go func() {
		stats := make(map[string]*RedisStats)
		for k, v := range fut.Wait() {
			stats[k] = v.(*RedisStats)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stats.servers = stats
	}()
	return &fut, nil
}

type ProxyCmdStats struct {
	CmdList []*proxy.CmdInfo `json:"cmd"`
}

type ProxyStats struct {
	Stats    *proxy.Stats     `json:"stats,omitempty"`
	Error    *rpc.RemoteError `json:"error,omitempty"`
	CmdStats *ProxyCmdStats   `json:"-"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) newProxyStats(p *models.Proxy, timeout time.Duration) *ProxyStats {
	var ch = make(chan struct{})
	stats := &ProxyStats{}

	go func() {
		defer close(ch)
		x, err := s.newProxyClient(p).StatsSimple()
		if err != nil {
			stats.Error = rpc.NewRemoteError(err)
		} else {
			stats.Stats = x
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return &ProxyStats{Timeout: true}
	}
}

func (s *Topom) newProxyCmdStats(p *models.Proxy, loops int64, timeout time.Duration) *proxy.CmdInfo {
	var ch = make(chan struct{})
	stats := &proxy.CmdInfo{}

	go func() {
		defer close(ch)
		x, err := s.newProxyClient(p).CmdInfo(loops)
		if err != nil {
			stats = nil
			//stats.Error = rpc.NewRemoteError(err)
		} else {
			stats = x
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return nil
	}
}

func (s *Topom) RefreshProxyStats(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			stats := s.newProxyStats(p, timeout)
			stats.UnixTime = time.Now().Unix()
			fut.Done(p.Token, stats)

			switch x := stats.Stats; {
			case x == nil:
			case x.Closed || x.Online:
			default:
				if err := s.OnlineProxy(p.AdminAddr); err != nil {
					log.WarnErrorf(err, "auto online proxy-[%s] failed", p.Token)
				}
			}
		}(p)
	}
	go func() {
		stats := make(map[string]*ProxyStats)
		for k, v := range fut.Wait() {
			stats[k] = v.(*ProxyStats)
			s.mu.Lock()
			if _, ok := s.stats.proxies[k]; ok {
				stats[k].CmdStats = s.stats.proxies[k].CmdStats
			}
			s.mu.Unlock()
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stats.proxies = stats
	}()
	return &fut, nil
}

type MastersAndSlavesStats struct {
	Error error `json:"error,omitempty"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) newMastersAndSlavesStats(timeout time.Duration, filter func(index int, g *models.GroupServer) bool, wg *sync.WaitGroup) *MastersAndSlavesStats {
	var ch = make(chan struct{})
	stats := &MastersAndSlavesStats{}
	defer wg.Done()

	go func() {
		defer close(ch)
		err := s.CheckStateAndSwitchSlavesAndMasters(filter)
		if err != nil {
			log.Errorf("refresh masters and slaves failed, %v", err)
			stats.Error = err
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return &MastersAndSlavesStats{Timeout: true}
	}
}

func (s *Topom) CheckMastersAndSlavesState(timeout time.Duration) (*sync.WaitGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go s.newMastersAndSlavesStats(timeout, func(index int, g *models.GroupServer) bool {
		return g.State == models.GroupServerStateNormal
	}, wg)
	return wg, nil
}

func (s *Topom) CheckPreOfflineMastersState(timeout time.Duration) (*sync.WaitGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go s.newMastersAndSlavesStats(timeout, func(index int, g *models.GroupServer) bool {
		return g.State == models.GroupServerStateSubjectiveOffline
	}, wg)
	return wg, nil
}

func (s *Topom) CheckOfflineMastersAndSlavesState(timeout time.Duration) (*sync.WaitGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go s.newMastersAndSlavesStats(timeout, func(index int, g *models.GroupServer) bool {
		return g.State == models.GroupServerStateOffline
	}, wg)
	return wg, nil
}

func (s *Topom) RefreshProxyCmdStats(timeout time.Duration, loops int64) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			proxyCmdStats := &ProxyCmdStats{}
			for i := 0; i < len(proxy.IntervalMark); i++ {
				if loops%proxy.IntervalMark[i] != 0 {
					proxyCmdStats.CmdList = append(proxyCmdStats.CmdList, nil)
					continue
				}

				stats := s.newProxyCmdStats(p, proxy.IntervalMark[i], timeout)
				if stats == nil {
					log.Warnf("newProxyCmdStats failed: proxy-[%s], interval-[%d]", p.ProxyAddr, proxy.IntervalMark[i])
				}

				proxyCmdStats.CmdList = append(proxyCmdStats.CmdList, stats)
			}

			fut.Done(p.Token, proxyCmdStats)
		}(p)
	}
	go func() {
		//stats := make(map[string]*ProxyStats)
		for k, v := range fut.Wait() {
			s.mu.Lock()
			//fmt.Println(k)
			proxyStats, ok := s.stats.proxies[k]
			if !ok {
				s.mu.Unlock()
				continue
			}
			if proxyStats.CmdStats == nil {
				proxyStats.CmdStats = &ProxyCmdStats{CmdList: make([]*proxy.CmdInfo, 5, 5)}
			}

			for i := 0; i < len(proxy.IntervalMark); i++ {
				cmdInfo := v.(*ProxyCmdStats).CmdList[i]
				// 如果到了统计周期但 cmdInfo 为 nil，说明此次统计结果获取失败
				if cmdInfo == nil && loops%proxy.IntervalMark[i] != 0 {
					continue
				}

				proxyStats.CmdStats.CmdList[i] = cmdInfo
			}
			s.mu.Unlock()
		}
	}()
	return &fut, nil
}
