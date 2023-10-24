package pika_integration

import (
	"context"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

func TestPika(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pika integration test")
}

var _ = BeforeSuite(func() {
	ctx := context.TODO()
	clientMaster := redis.NewClient(pikaOptions1())
	clientSlave := redis.NewClient(pikaOptions2())
	cleanEnv(ctx, clientSlave, clientMaster)
	Expect(clientSlave.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	Expect(clientMaster.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	time.Sleep(5 * time.Second)
})
