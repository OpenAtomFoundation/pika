// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"time"

	etcdclient "pika/codis/v2/pkg/models/etcd"
	fsclient "pika/codis/v2/pkg/models/fs"
	zkclient "pika/codis/v2/pkg/models/zk"
	"pika/codis/v2/pkg/utils/errors"
)

type Client interface {
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error

	Read(path string, must bool) ([]byte, error)
	List(path string, must bool) ([]string, error)

	Close() error

	WatchInOrder(path string) (<-chan struct{}, []string, error)

	CreateEphemeral(path string, data []byte) (<-chan struct{}, error)
	CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error)
}

func NewClient(coordinator string, addrlist string, auth string, timeout time.Duration) (Client, error) {
	switch coordinator {
	case "zk", "zookeeper":
		return zkclient.New(addrlist, auth, timeout)
	case "etcd":
		return etcdclient.New(addrlist, auth, timeout)
	case "fs", "filesystem":
		return fsclient.New(addrlist)
	}
	return nil, errors.Errorf("invalid coordinator name = %s", coordinator)
}
