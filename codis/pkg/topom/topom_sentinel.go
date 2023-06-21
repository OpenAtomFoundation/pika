// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"

	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/redis"
)

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
	for gid, servers := range ctx.getGroupServers() {
		for i, server := range servers {
			if filter(i, server) {
				if val, ok := gs[gid]; ok {
					gs[gid] = append(val, server)
				} else {
					gs[gid] = []*models.GroupServer{server}
				}
			}
		}
	}
	if len(gs) == 0 {
		return nil
	}

	states := sentinel.RefreshMastersAndSlavesClient(config.ParallelSyncs, gs)

	var pending []*models.Group

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
			if g.Servers[0].State == models.GroupServerStateNormal {
				g.Servers[0].State = models.GroupServerStateSubjectiveOffline
			} else {
				// 更新重试次数
				g.Servers[0].ReCallTimes++

				// 重试超过5次，开始选主
				if g.Servers[0].ReCallTimes >= 5 {
					// 标记进入客观下线状态
					g.Servers[0].State = models.GroupServerStateOffline
				}
				// 开始进行选主
				if g.Servers[0].State == models.GroupServerStateOffline {
					pending = append(pending, g)
				}
			}
		}

		// 更新state、role、offset信息
		if val, ok := serversMap[state.Addr]; ok {
			if state.Err != nil {
				if val.State == models.GroupServerStateNormal {
					val.State = models.GroupServerStateSubjectiveOffline
				}
				continue
			}

			val.State = models.GroupServerStateNormal
			val.ReCallTimes = 0
			val.Role = state.Replication.Role
			if val.Role == "master" {
				val.ReplyOffset = state.Replication.MasterReplOffset
			} else {
				val.ReplyOffset = state.Replication.SlaveReplOffset
			}
		}
	}

	if len(pending) == 0 {
		return nil
	}

	cache := &redis.InfoCache{
		Auth: s.config.ProductAuth, Timeout: time.Millisecond * 100,
	}
	// 尝试进行主从切换
	for _, g := range pending {
		if err = s.trySwitchGroupMaster2(g.Id, cache); err != nil {
			log.Errorf("gid-[%d] switch master failed, %v", g.Id, err)
			continue
		}

		slots := ctx.getSlotMappingsByGroupId(g.Id)
		// 通知所有的server更新slot的信息
		if err = s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to preparing", g.Id)
			continue
		}
		s.dirtyGroupCache(g.Id)
	}

	return nil
}
