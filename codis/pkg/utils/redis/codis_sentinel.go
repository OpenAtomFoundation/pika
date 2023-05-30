package redis

import (
	"context"
	"fmt"
	"pika/codis/v2/pkg/models"
	"pika/codis/v2/pkg/utils/errors"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/math2"
	"pika/codis/v2/pkg/utils/sync2"
	"time"
)

type CodisSentinel struct {
	context.Context
	Cancel context.CancelFunc

	Product, Auth string

	LogFunc func(format string, args ...interface{})
	ErrFunc func(err error, format string, args ...interface{})
}

func NewCodisSentinel(product, auth string) *CodisSentinel {
	s := &CodisSentinel{Product: product, Auth: auth}
	s.Context, s.Cancel = context.WithCancel(context.Background())
	return s
}

func (s *CodisSentinel) IsCanceled() bool {
	select {
	case <-s.Context.Done():
		return true
	default:
		return false
	}
}

func (s *CodisSentinel) printf(format string, args ...interface{}) {
	if s.LogFunc != nil {
		s.LogFunc(format, args...)
	}
}

func (s *CodisSentinel) errorf(err error, format string, args ...interface{}) {
	if s.ErrFunc != nil {
		s.ErrFunc(err, format, args...)
	}
}

func (s *CodisSentinel) do(sentinel string, timeout time.Duration,
	fn func(client *Client) error) error {
	c, err := NewClientNoAuth(sentinel, timeout)
	if err != nil {
		return err
	}
	defer c.Close()
	return fn(c)
}

func (s *CodisSentinel) dispatch(ctx context.Context, sentinel string, timeout time.Duration,
	fn func(client *Client) error) error {
	c, err := NewClientNoAuth(sentinel, timeout)
	if err != nil {
		return err
	}
	defer c.Close()

	var exit = make(chan error, 1)

	go func() {
		exit <- fn(c)
	}()

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-exit:
		return err
	}
}

// todo timeout 参数是从配置文件读取的，需要修改配置文件里面参数的名字
//todo 添加参数，表示多久检查一次是否存活
func (s *CodisSentinel) RefreshMastersAndSlavesClient(parallel int, groupServers map[int][]*models.GroupServer) []*ReplicationState {
	if len(groupServers) == 0 {
		s.printf("there's no groups")
		return nil
	}

	parallel = math2.MaxInt(10, parallel)
	limit := make(chan struct{}, parallel)
	defer close(limit)

	var fut sync2.Future

	for gid, servers := range groupServers {
		for index, server := range servers {
			limit <- struct{}{}
			fut.Add()

			go func(gid, index int, server *models.GroupServer) {
				defer func() {
					<-limit
				}()

				info, err := s.infoReplicationDispatch(server.Addr)
				state := &ReplicationState{
					Index:       index,
					GroupID:     gid,
					Addr:        server.Addr,
					Replication: info,
					Err:         err,
				}
				fut.Done(fmt.Sprintf("%d_%d", gid, index), state)
			}(gid, index, server)
		}
	}

	results := make([]*ReplicationState, 0)

	for _, v := range fut.Wait() {
		switch val := v.(type) {
		case *ReplicationState:
			if val != nil {
				results = append(results, val)
			}
		}
	}

	return results
}

func (s *CodisSentinel) infoReplicationDispatch(addr string) (*InfoReplication, error) {
	// 这两步如果失败，需要做特殊处理
	// todo 待确认这里是否使用pool？
	if c, err := NewClient(addr, s.Auth, time.Second); err != nil {
		log.WarnErrorf(err, "create redis client to %s failed", addr)
		return nil, err
	} else {
		defer c.Close()
		return c.InfoReplication()
	}
}
