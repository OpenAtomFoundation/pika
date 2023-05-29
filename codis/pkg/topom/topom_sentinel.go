// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"

	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/utils/errors"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/math2"
	"pika/codis/v2/pkg/utils/redis"
	"pika/codis/v2/pkg/utils/sync2"
)

func (s *Topom) AddSentinel(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid sentinel address")
	}
	p := ctx.sentinel

	for _, x := range p.Servers {
		if x == addr {
			return errors.Errorf("sentinel-[%s] already exists", addr)
		}
	}

	sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
	if err := sentinel.FlushConfig(addr, s.config.SentinelClientTimeout.Duration()); err != nil {
		return err
	}
	defer s.dirtySentinelCache()

	p.Servers = append(p.Servers, addr)
	p.OutOfSync = true
	return s.storeUpdateSentinel(p)
}

func (s *Topom) DelSentinel(addr string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid sentinel address")
	}
	p := ctx.sentinel

	var slice []string
	for _, x := range p.Servers {
		if x != addr {
			slice = append(slice, x)
		}
	}
	if len(slice) == len(p.Servers) {
		return errors.Errorf("sentinel-[%s] not found", addr)
	}
	defer s.dirtySentinelCache()

	p.OutOfSync = true
	if err := s.storeUpdateSentinel(p); err != nil {
		return err
	}

	sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
	if err := sentinel.RemoveGroupsAll([]string{addr}, s.config.SentinelClientTimeout.Duration()); err != nil {
		log.WarnErrorf(err, "remove sentinel %s failed", addr)
		if !force {
			return errors.Errorf("remove sentinel %s failed", addr)
		}
	}

	p.Servers = slice
	return s.storeUpdateSentinel(p)
}

func (s *Topom) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedTopom
	}
	s.ha.masters = masters

	if len(masters) != 0 {
		cache := &redis.InfoCache{
			Auth: s.config.ProductAuth, Timeout: time.Millisecond * 100,
		}
		for gid, master := range masters {
			if err := s.trySwitchGroupMaster(gid, master, cache); err != nil {
				log.WarnErrorf(err, "sentinel switch group master failed")
			}
		}
	}
	return nil
}

// todo 这里不再试用
func (s *Topom) rewatchSentinels(servers []string) {
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
		s.ha.monitor = nil
	}
	if len(servers) == 0 {
		s.ha.masters = nil
	} else {
		s.ha.monitor = redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
		s.ha.monitor.LogFunc = log.Warnf
		s.ha.monitor.ErrFunc = log.WarnErrorf
		go func(p *redis.Sentinel) {
			var trigger = make(chan struct{}, 1)
			delayUntil := func(deadline time.Time) {
				for !p.IsCanceled() {
					var d = deadline.Sub(time.Now())
					if d <= 0 {
						return
					}
					time.Sleep(math2.MinDuration(d, time.Second))
				}
			}
			go func() {
				defer close(trigger)
				callback := func() {
					select {
					case trigger <- struct{}{}:
					default:
					}
				}
				for !p.IsCanceled() {
					timeout := time.Minute * 15
					retryAt := time.Now().Add(time.Second * 10)
					if !p.Subscribe(servers, timeout, callback) {
						delayUntil(retryAt)
					} else {
						callback()
					}
				}
			}()
			go func() {
				for range trigger {
					var success int
					for i := 0; i != 10 && !p.IsCanceled() && success != 2; i++ {
						timeout := time.Second * 5
						masters, err := p.Masters(servers, timeout)
						if err != nil {
							log.WarnErrorf(err, "fetch group masters failed")
						} else {
							if !p.IsCanceled() {
								s.SwitchMasters(masters)
							}
							success += 1
						}
						delayUntil(time.Now().Add(time.Second * 5))
					}
				}
			}()
		}(s.ha.monitor)
	}
	log.Warnf("rewatch sentinels = %v", servers)
}

func (s *Topom) CheckAndSwitchSlavesAndMasters(filter func(index int, g *models.GroupServer) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	config := &redis.MonitorConfig{
		Quorum:               s.config.SentinelQuorum,
		ParallelSyncs:        s.config.SentinelParallelSyncs,
		DownAfter:            s.config.SentinelDownAfter.Duration(),
		FailoverTimeout:      s.config.SentinelFailoverTimeout.Duration(),
		NotificationScript:   s.config.SentinelNotificationScript,
		ClientReconfigScript: s.config.SentinelClientReconfigScript,
	}

	sentinel := redis.NewCodisSentinel(s.config.ProductName, s.config.ProductAuth)
	gs := make(map[int][]*models.GroupServer)
	for i, servers := range ctx.getGroupServers() {
		for _, server := range servers {
			if filter(i, server) {
				if val, ok := gs[i]; ok {
					gs[i] = append(val, server)
				} else {
					gs[i] = []*models.GroupServer{server}
				}
			}
		}
	}
	if len(gs) == 0 {
		return nil
	}

	states := sentinel.RefreshMastersAndSlavesClient(config.ParallelSyncs, gs)

	var changedGroups []*models.Group

	for _, state := range states {
		var g *models.Group
		if g, err = ctx.getGroup(state.GroupID); err != nil {
			return err
		}

		serversMap := g.GetServersMap()
		if len(serversMap) == 0 {
			continue
		}

		// 之前是主节点 && 主节点挂机 && 当前还是主节点
		if state.Index == 0 && state.Err != nil && g.Servers[0].Addr == state.Addr {
			// 修改状态为主观下线
			if g.Servers[0].State == 0 {
				g.Servers[0].State = 1
			} else if g.Servers[0].State == 2 {
				// 客观下线，开始选主
				changedGroups = append(changedGroups, g)
			} else {
				// 主观下线，更新重试次数
				g.Servers[0].ReCallTimes++
				if g.Servers[0].ReCallTimes >= 5 {
					// 重试超过5次，开始选主
					changedGroups = append(changedGroups, g)
					// 标记进入客观下线状态
					g.Servers[0].State = 2
				}
			}
		}

		// 更新state、role、offset信息
		if val, ok := serversMap[state.Addr]; ok {
			// 如果是进入了"客观下线"状态，需要人工介入才能恢复正常服务
			if state.Err != nil || val.State == 2 {
				if val.State == 0 {
					val.State = 1
				}
				continue
			}

			val.State = 0
			val.ReCallTimes = 0
			val.Role = state.Replication.Role
			if state.Replication.Role == "master" {
				val.ReplyOffset = state.Replication.MasterReplOffset
			} else {
				val.ReplyOffset = state.Replication.SlaveReplOffset
			}
		}
	}

	if len(changedGroups) == 0 {
		return nil
	}

	cache := &redis.InfoCache{
		Auth: s.config.ProductAuth, Timeout: time.Millisecond * 100,
	}
	// 尝试进行主从切换
	for _, g := range changedGroups {
		if err = s.trySwitchGroupMaster2(g.Id, cache); err != nil {
			log.Error("gid-[%d] switch master failed, %v", g.Id, err)
			continue
		}

		slots := ctx.getSlotMappingsByGroupId(g.Id)
		// 通知所有的server更新slot的信息
		if err = s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to preparing", g.Id)
			continue
		}
		defer s.dirtyGroupCache(g.Id)
	}

	return nil
}

func (s *Topom) ResyncSentinels() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	defer s.dirtySentinelCache()

	p := ctx.sentinel
	p.OutOfSync = true
	if err := s.storeUpdateSentinel(p); err != nil {
		return err
	}

	config := &redis.MonitorConfig{
		Quorum:               s.config.SentinelQuorum,
		ParallelSyncs:        s.config.SentinelParallelSyncs,
		DownAfter:            s.config.SentinelDownAfter.Duration(),
		FailoverTimeout:      s.config.SentinelFailoverTimeout.Duration(),
		NotificationScript:   s.config.SentinelNotificationScript,
		ClientReconfigScript: s.config.SentinelClientReconfigScript,
	}

	sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
	if err := sentinel.RemoveGroupsAll(p.Servers, s.config.SentinelClientTimeout.Duration()); err != nil {
		log.WarnErrorf(err, "remove sentinels failed")
	}
	if err := sentinel.MonitorGroups(p.Servers, s.config.SentinelClientTimeout.Duration(), config, ctx.getGroupMasters()); err != nil {
		log.WarnErrorf(err, "resync sentinels failed")
		return err
	}
	s.rewatchSentinels(p.Servers)

	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(p).SetSentinels(ctx.sentinel)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync sentinel failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] sentinel failed", t)
			}
		}
	}

	p.OutOfSync = false
	return s.storeUpdateSentinel(p)
}
