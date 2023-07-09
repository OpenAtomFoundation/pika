// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"encoding/json"
	"time"

	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/utils/errors"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/redis"
)

func (s *Topom) CreateGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if gid <= 0 || gid > models.MaxGroupId {
		return errors.Errorf("invalid group id = %d, out of range", gid)
	}
	if ctx.group[gid] != nil {
		return errors.Errorf("group-[%d] already exists", gid)
	}
	defer s.dirtyGroupCache(gid)

	g := &models.Group{
		Id:      gid,
		Servers: []*models.GroupServer{},
	}
	return s.storeCreateGroup(g)
}

func (s *Topom) RemoveGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if len(g.Servers) != 0 {
		return errors.Errorf("group-[%d] isn't empty", gid)
	}
	defer s.dirtyGroupCache(g.Id)

	return s.storeRemoveGroup(g)
}

func (s *Topom) ResyncGroup(gid int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	if err := s.resyncSlotMappingsByGroupId(ctx, gid); err != nil {
		log.Warnf("group-[%d] resync-group failed", g.Id)
		return err
	}
	defer s.dirtyGroupCache(gid)

	g.OutOfSync = false
	return s.storeUpdateGroup(g)
}

func (s *Topom) ResyncGroupAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, g := range ctx.group {
		if err := s.resyncSlotMappingsByGroupId(ctx, g.Id); err != nil {
			log.Warnf("group-[%d] resync-group failed", g.Id)
			return err
		}
		defer s.dirtyGroupCache(g.Id)

		g.OutOfSync = false
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) GroupAddServer(gid int, dc, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	if addr == "" {
		return errors.Errorf("invalid server address")
	}

	for _, g := range ctx.group {
		for _, x := range g.Servers {
			if x.Addr == addr {
				return errors.Errorf("server-[%s] already exists", addr)
			}
		}
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if p := ctx.sentinel; len(p.Servers) != 0 {
		defer s.dirtySentinelCache()
		p.OutOfSync = true
		if err := s.storeUpdateSentinel(p); err != nil {
			return err
		}
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers = append(g.Servers, &models.GroupServer{Addr: addr, DataCenter: dc})
	return s.storeUpdateGroup(g)
}

func (s *Topom) GroupDelServer(gid int, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if index == 0 {
		if len(g.Servers) != 1 || ctx.isGroupInUse(g.Id) {
			return errors.Errorf("group-[%d] can't remove master, still in use", g.Id)
		}
	}

	if p := ctx.sentinel; len(p.Servers) != 0 {
		defer s.dirtySentinelCache()
		p.OutOfSync = true
		if err := s.storeUpdateSentinel(p); err != nil {
			return err
		}
	}
	defer s.dirtyGroupCache(g.Id)

	if index != 0 && g.Servers[index].ReplicaGroup {
		g.OutOfSync = true
	}

	var slice = make([]*models.GroupServer, 0, len(g.Servers))
	for i, x := range g.Servers {
		if i != index {
			slice = append(slice, x)
		}
	}
	if len(slice) == 0 {
		g.OutOfSync = false
	}

	g.Servers = slice

	return s.storeUpdateGroup(g)
}

func (s *Topom) GroupPromoteServer(gid int, addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		if index != g.Promoting.Index {
			return errors.Errorf("group-[%d] is promoting index = %d", g.Id, g.Promoting.Index)
		}
	} else {
		if index == 0 {
			return errors.Errorf("group-[%d] can't promote master", g.Id)
		}
	}
	if n := s.action.executor.Int64(); n != 0 {
		return errors.Errorf("slots-migration is running = %d", n)
	}

	switch g.Promoting.State {

	case models.ActionNothing:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] will promote index = %d", g.Id, index)

		g.Promoting.Index = index
		g.Promoting.State = models.ActionPreparing
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		fallthrough

	case models.ActionPreparing:

		defer s.dirtyGroupCache(g.Id)

		log.Warnf("group-[%d] resync to prepared", g.Id)

		slots := ctx.getSlotMappingsByGroupId(g.Id)

		g.Promoting.State = models.ActionPrepared
		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync-rollback to preparing", g.Id)
			g.Promoting.State = models.ActionPreparing
			s.resyncSlotMappings(ctx, slots...)
			log.Warnf("group-[%d] resync-rollback to preparing, done", g.Id)
			return err
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		fallthrough

	case models.ActionPrepared:

		if p := ctx.sentinel; len(p.Servers) != 0 {
			defer s.dirtySentinelCache()
			p.OutOfSync = true
			if err := s.storeUpdateSentinel(p); err != nil {
				return err
			}
			if s.ha.masters != nil {
				delete(s.ha.masters, gid)
			}
		}

		defer s.dirtyGroupCache(g.Id)

		var index = g.Promoting.Index
		var slice = make([]*models.GroupServer, 0, len(g.Servers))
		slice = append(slice, g.Servers[index])
		for i, x := range g.Servers {
			if i != index && i != 0 {
				slice = append(slice, x)
			}
		}
		slice = append(slice, g.Servers[0])

		for _, x := range slice {
			x.Action.Index = 0
			x.Action.State = models.ActionNothing
		}

		g.Servers = slice
		g.Promoting.Index = 0
		g.Promoting.State = models.ActionFinished
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}

		var (
			master = slice[0].Addr
			client *redis.Client
		)
		if client, err = redis.NewClient(master, s.config.ProductAuth, time.Second); err != nil {
			log.WarnErrorf(err, "create redis client to %s failed", master)
		}
		defer client.Close()
		if err = client.SetMaster("NO:ONE"); err != nil {
			log.WarnErrorf(err, "redis %s set master to NO:ONE failed", master)
		}
		fallthrough

	case models.ActionFinished:

		log.Warnf("group-[%d] resync to finished", g.Id)

		slots := ctx.getSlotMappingsByGroupId(g.Id)

		if err := s.resyncSlotMappings(ctx, slots...); err != nil {
			log.Warnf("group-[%d] resync to finished failed", g.Id)
			return err
		}
		defer s.dirtyGroupCache(g.Id)

		g = &models.Group{
			Id:      g.Id,
			Servers: g.Servers,
		}
		return s.storeUpdateGroup(g)

	default:

		return errors.Errorf("group-[%d] action state is invalid", gid)

	}
}

func (s *Topom) trySwitchGroupMaster(gid int, cache *redis.InfoCache) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	master := s.selectNextMaster(g.Servers)

	if master == "" {
		servers, _ := json.Marshal(g)
		log.Errorf("group %d donn't has any slaves to switch master, %s", gid, servers)
		return errors.Errorf("cann't switch slave to master")
	}

	return s.doSwitchGroupMaster(gid, master, cache)
}

// Choose to change to the next master node in the group
func (s *Topom) selectNextMaster(servers []*models.GroupServer) string {
	if len(servers) == 0 {
		return ""
	}

	var masterServer *models.GroupServer

	for _, server := range servers {
		if server.State != models.GroupServerStateNormal {
			continue
		}

		// If there is already a master node in the group working normally, return directly
		if server.Role == "master" {
			return server.Addr
		}

		if masterServer == nil {
			masterServer = server
		} else if server.ReplyOffset > masterServer.ReplyOffset {
			// Select the slave node with the latest offset as the master node
			masterServer = server
		}
	}

	if masterServer == nil {
		return ""
	}

	return masterServer.Addr
}

func (s *Topom) doSwitchGroupMaster(gid int, master string, cache *redis.InfoCache) error {
	ctx, err := s.newContext()
	if err != nil {
		return err
	}
	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}

	var index = func() int {
		for i, x := range g.Servers {
			if x.Addr == master {
				return i
			}
		}
		for i, x := range g.Servers {
			rid1 := cache.GetRunId(master)
			rid2 := cache.GetRunId(x.Addr)
			if rid1 != "" && rid1 == rid2 {
				return i
			}
		}
		return -1
	}()
	if index == -1 {
		return errors.Errorf("group-[%d] doesn't have server %s with runid = '%s'", g.Id, master, cache.GetRunId(master))
	}
	if index == 0 {
		return nil
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("group-[%d] will switch master to server[%d] = %s", g.Id, index, g.Servers[index].Addr)

	// Set the slave node as the new master node
	var client *redis.Client
	if client, err = redis.NewClient(master, s.config.ProductAuth, 100*time.Millisecond); err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", master)
		return err
	}

	defer client.Close()
	if err = client.SetMaster("NO:ONE"); err != nil {
		log.WarnErrorf(err, "redis %s set master to NO:ONE failed", master)
		return err
	}

	// Set other nodes in the group as slave nodes of the new master node
	for _, server := range g.Servers {
		if server.State != models.GroupServerStateNormal || server.Addr == master {
			continue
		}
		var client2 *redis.Client
		if client2, err = redis.NewClient(server.Addr, s.config.ProductAuth, 100*time.Millisecond); err != nil {
			log.WarnErrorf(err, "create redis client to %s failed", master)
			return err
		}
		defer client2.Close()
		if err = client2.SetMaster(master); err != nil {
			log.WarnErrorf(err, "redis %s set master to %s failed", server.Addr, master)
			return err
		}
	}

	g.Servers[0], g.Servers[index] = g.Servers[index], g.Servers[0]
	g.Servers[0].Role = "master"
	g.OutOfSync = true
	return s.storeUpdateGroup(g)
}

func (s *Topom) EnableReplicaGroups(gid int, addr string, value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, err := ctx.getGroup(gid)
	if err != nil {
		return err
	}
	index, err := ctx.getGroupIndex(g, addr)
	if err != nil {
		return err
	}

	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}
	defer s.dirtyGroupCache(g.Id)

	if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
		g.OutOfSync = true
	}
	g.Servers[index].ReplicaGroup = value

	return s.storeUpdateGroup(g)
}

func (s *Topom) EnableReplicaGroupsAll(value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	for _, g := range ctx.group {
		if g.Promoting.State != models.ActionNothing {
			return errors.Errorf("group-[%d] is promoting", g.Id)
		}
		defer s.dirtyGroupCache(g.Id)

		var dirty bool
		for _, x := range g.Servers {
			if x.ReplicaGroup != value {
				x.ReplicaGroup = value
				dirty = true
			}
		}
		if !dirty {
			continue
		}
		if len(g.Servers) != 1 && ctx.isGroupInUse(g.Id) {
			g.OutOfSync = true
		}
		if err := s.storeUpdateGroup(g); err != nil {
			return err
		}
	}
	return nil
}

func (s *Topom) SyncCreateAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionPending {
		return errors.Errorf("server-[%s] action already exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = ctx.maxSyncActionIndex() + 1
	g.Servers[index].Action.State = models.ActionPending
	return s.storeUpdateGroup(g)
}

func (s *Topom) SyncRemoveAction(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return err
	}
	if g.Promoting.State != models.ActionNothing {
		return errors.Errorf("group-[%d] is promoting", g.Id)
	}

	if g.Servers[index].Action.State == models.ActionNothing {
		return errors.Errorf("server-[%s] action doesn't exist", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	g.Servers[index].Action.Index = 0
	g.Servers[index].Action.State = models.ActionNothing
	return s.storeUpdateGroup(g)
}

func (s *Topom) SyncActionPrepare() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return "", err
	}

	addr := ctx.minSyncActionIndex()
	if addr == "" {
		return "", nil
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return "", err
	}
	if g.Promoting.State != models.ActionNothing {
		return "", nil
	}

	if g.Servers[index].Action.State != models.ActionPending {
		return "", errors.Errorf("server-[%s] action state is invalid", addr)
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("server-[%s] action prepare", addr)

	g.Servers[index].Action.Index = 0
	g.Servers[index].Action.State = models.ActionSyncing
	return addr, s.storeUpdateGroup(g)
}

func (s *Topom) SyncActionComplete(addr string, failed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return nil
	}
	if g.Promoting.State != models.ActionNothing {
		return nil
	}

	if g.Servers[index].Action.State != models.ActionSyncing {
		return nil
	}
	defer s.dirtyGroupCache(g.Id)

	log.Warnf("server-[%s] action failed = %t", addr, failed)

	var state string
	if !failed {
		state = "synced"
	} else {
		state = "synced_failed"
	}
	g.Servers[index].Action.State = state
	return s.storeUpdateGroup(g)
}

func (s *Topom) newSyncActionExecutor(addr string) (func() error, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	g, index, err := ctx.getGroupByServer(addr)
	if err != nil {
		return nil, nil
	}

	if g.Servers[index].Action.State != models.ActionSyncing {
		return nil, nil
	}

	var master = "NO:ONE"
	if index != 0 {
		master = g.Servers[0].Addr
	}
	return func() error {
		c, err := redis.NewClient(addr, s.config.ProductAuth, time.Minute*30)
		if err != nil {
			log.WarnErrorf(err, "create redis client to %s failed", addr)
			return err
		}
		defer c.Close()
		if err := c.SetMaster(master); err != nil {
			log.WarnErrorf(err, "redis %s set master to %s failed", addr, master)
			return err
		}
		return nil
	}, nil
}
