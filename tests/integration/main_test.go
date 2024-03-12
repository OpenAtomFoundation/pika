package pika_integration

import (
	"context"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"testing"
)

var (
	GlobalBefore func(ctx context.Context, client *redis.Client)
)

func TestPikaWithCache(t *testing.T) {
	GlobalBefore = func(ctx context.Context, client *redis.Client) {
		Expect(client.SlaveOf(ctx, "NO", "ONE").Err()).NotTo(HaveOccurred())
		Expect(client.FlushAll(ctx).Err()).NotTo(HaveOccurred())
		Expect(client.ConfigSet(ctx, "cache-model", "1").Err()).NotTo(HaveOccurred())
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pika integration test with cache")
}

func TestPikaWithoutCache(t *testing.T) {
	GlobalBefore = func(ctx context.Context, client *redis.Client) {
		Expect(client.SlaveOf(ctx, "NO", "ONE").Err()).NotTo(HaveOccurred())
		Expect(client.FlushAll(ctx).Err()).NotTo(HaveOccurred())
		Expect(client.ConfigSet(ctx, "cache-model", "0").Err()).NotTo(HaveOccurred())
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pika integration test without cache")
}
