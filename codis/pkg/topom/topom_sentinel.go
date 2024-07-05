// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/utils/redis"
)

func (s *Topom) CheckStateAndSwitchSlavesAndMasters(filter func(index int, g *models.GroupServer) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	groupServers := filterGroupServer(ctx.getGroupServers(), filter)
	if len(groupServers) == 0 {
		return nil
	}

	states := checkGroupServersReplicationState(s.Config(), groupServers)
	var slaveOfflineGroups []*models.Group
	var masterOfflineGroups []*models.Group
	var recoveredGroupServersState []*redis.ReplicationState
	var group *models.Group
	for _, state := range states {
		group, err = ctx.getGroup(state.GroupID)
		if err != nil {
			return err
		}

		s.checkAndUpdateGroupServerState(s.Config(), group, state.Server, state, &slaveOfflineGroups,
			&masterOfflineGroups, &recoveredGroupServersState)
	}

	if len(slaveOfflineGroups) > 0 {
		// slave has been offline, and update state
		s.updateSlaveOfflineGroups(ctx, slaveOfflineGroups)
	}

	if len(masterOfflineGroups) > 0 {
		// old master offline already, auto switch to new master
		s.trySwitchGroupsToNewMaster(ctx, masterOfflineGroups)
	}

	if len(recoveredGroupServersState) > 0 {
		// offline GroupServer's service has recovered, check and fix it's master-slave replication relationship
		s.tryFixReplicationRelationships(ctx, recoveredGroupServersState,len(masterOfflineGroups))
	}

	return nil
}

func (s *Topom) checkAndUpdateGroupServerState(conf *Config, group *models.Group, groupServer *models.GroupServer,
	state *redis.ReplicationState, slaveOfflineGroups *[]*models.Group, masterOfflineGroups *[]*models.Group,
	recoveredGroupServers *[]*redis.ReplicationState) {
	if state.Err != nil {
		if groupServer.State == models.GroupServerStateNormal {
			// pre offline
			groupServer.State = models.GroupServerStateSubjectiveOffline
		} else {
			// update retries
			groupServer.ReCallTimes++

			// Retry more than config times, start election
			if groupServer.ReCallTimes >= conf.SentinelMasterDeadCheckTimes {
				// Mark enters objective offline state
				groupServer.State = models.GroupServerStateOffline
				groupServer.Action.State = models.ActionNothing
				groupServer.ReplicaGroup = false
			}

			// Start the election master node
			if groupServer.State == models.GroupServerStateOffline && isGroupMaster(state, group) {
				*masterOfflineGroups = append(*masterOfflineGroups, group)
			} else {
				*slaveOfflineGroups = append(*slaveOfflineGroups, group)
			}
		}
	} else {
		if groupServer.State == models.GroupServerStateOffline {
			*recoveredGroupServers = append(*recoveredGroupServers, state)
			// update GroupServer to GroupServerStateNormal state later
		} else {
			// Update the offset information of the state and role nodes
			groupServer.State = models.GroupServerStateNormal
			groupServer.ReCallTimes = 0
			groupServer.ReplicaGroup = true
			groupServer.Role = models.GroupServerRole(state.Replication.Role)
			groupServer.DbBinlogFileNum = state.Replication.DbBinlogFileNum
			groupServer.DbBinlogOffset = state.Replication.DbBinlogOffset
			groupServer.Action.State = models.ActionSynced
		}
	}
}

func checkGroupServersReplicationState(conf *Config, gs map[int][]*models.GroupServer) []*redis.ReplicationState {
	config := &redis.MonitorConfig{
		Quorum:               conf.SentinelQuorum,
		ParallelSyncs:        conf.SentinelParallelSyncs,
		DownAfter:            conf.SentinelDownAfter.Duration(),
		FailoverTimeout:      conf.SentinelFailoverTimeout.Duration(),
		NotificationScript:   conf.SentinelNotificationScript,
		ClientReconfigScript: conf.SentinelClientReconfigScript,
	}

	sentinel := redis.NewCodisSentinel(conf.ProductName, conf.ProductAuth)
	return sentinel.RefreshMastersAndSlavesClient(config.ParallelSyncs, gs)
}

func filterGroupServer(groupServers map[int][]*models.GroupServer,
	filter func(index int, gs *models.GroupServer) bool) map[int][]*models.GroupServer {
	filteredGroupServers := make(map[int][]*models.GroupServer)
	for gid, servers := range groupServers {
		for i, server := range servers {
			if filter(i, server) {
				if val, ok := filteredGroupServers[gid]; ok {
					filteredGroupServers[gid] = append(val, server)
				} else {
					filteredGroupServers[gid] = []*models.GroupServer{server}
				}
			}
		}
	}
	return filteredGroupServers
}
