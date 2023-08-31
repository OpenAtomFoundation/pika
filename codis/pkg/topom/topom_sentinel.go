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

		// It was the master node before, the master node hangs up, and it is currently the master node
		if state.Index == 0 && state.Err != nil && g.Servers[0].Addr == state.Addr {
			if g.Servers[0].State == models.GroupServerStateNormal {
				g.Servers[0].State = models.GroupServerStateSubjectiveOffline
			} else {
				// update retries
				g.Servers[0].ReCallTimes++

				// Retry more than config times, start election
				if g.Servers[0].ReCallTimes >= s.Config().SentinelMasterDeadCheckTimes {
					// Mark enters objective offline state
					g.Servers[0].State = models.GroupServerStateOffline
					g.Servers[0].ReplicaGroup = false
				}
				// Start the election master node
				if g.Servers[0].State == models.GroupServerStateOffline {
					pending = append(pending, g)
				}
			}
		}

		// Update the offset information of the state and role nodes
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
	// Try to switch master slave
	for _, g := range pending {
		if err = s.trySwitchGroupMaster(g.Id, cache); err != nil {
			log.Errorf("gid-[%d] switch master failed, %v", g.Id, err)
			continue
		}

		slots := ctx.getSlotMappingsByGroupId(g.Id)
		// Notify all servers to update slot information
		if err = s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to preparing", g.Id)
			continue
		}
		s.dirtyGroupCache(g.Id)
	}

	return nil
}
