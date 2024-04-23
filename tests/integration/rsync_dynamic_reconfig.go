package pika_integration

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

func RefillMaster(masterAddr string, dataVolumeMB int64, ctx context.Context) {
	//the datavolumeMB could not be too large(like 1024MB) or refill shall take a long time to finish
	genRandomStr := func(n int, tId int) string {
		letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		bytes := make([]byte, n)
		for i := range bytes {
			index := (rand.Intn(1000) + tId) % len(letters)
			bytes[i] = letters[index]
		}
		return string(bytes)
	}
	writeFun := func(targetAddr string, requestNum int64, wg *sync.WaitGroup, tId int) {
		defer wg.Done()
		cli := redis.NewClient(PikaOption(targetAddr))
		defer cli.Close()
		var i int64
		for i = 0; i < requestNum; i++ {
			rKey := genRandomStr(1024, tId)
			rValue := genRandomStr(1024, tId)
			cli.Set(ctx, rKey, rValue, 0)
		}
	}
	keySize := 1024
	valueSize := 1024
	dataVolumeBytes := dataVolumeMB << 20
	threadNum := 10
	reqNumForEachThead := dataVolumeBytes / int64((keySize + valueSize)) / int64(threadNum)
	//fmt.Printf("reqNumForEach:%d\n", reqNumForEachThead)
	startTime := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go writeFun(masterAddr, reqNumForEachThead, &wg, i)
	}
	wg.Wait()
	duration := time.Since(startTime)
	fmt.Printf("RefillMaster took %s to complete.\n", duration)
}

func ReleaseRsyncLimit(cli *redis.Client, ctx context.Context) {
	//sleep is needed, because the update frequency limit for rsync config is 1 time per 2s
	time.Sleep(time.Second * 2)
	//fmt.Println("removing rsync limimt")
	if err := cli.ConfigSet(ctx, "rsync-timeout-ms", "1000").Err(); err != nil {
		fmt.Println("Error setting key:", err)
		return
	}
	time.Sleep(time.Second * 2)
	bigRate := 1 << 30 //1GB
	if err := cli.ConfigSet(ctx, "throttle-bytes-per-second", strconv.Itoa(bigRate)).Err(); err != nil {
		fmt.Println("Error setting key:", err)
		return
	}
	fmt.Println("rsync limit is removed")
}

func UpdateThrottle(cli *redis.Client, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := cli.ConfigSet(ctx, "throttle-bytes-per-second", "65535").Err(); err != nil {
		fmt.Println("Error setting key:", err)
		return
	}
	time.Sleep(time.Second * 3)
	rand.Seed(time.Now().UnixNano())
	for i := 1; i < 200; i++ {
		time.Sleep(time.Millisecond * 300)
		min := 512 << 10 //512 KB
		max := 5 << 20   //5 MB
		randomInt := rand.Intn(max-min+1) + min
		//do the update throttle bytes, randomly from 64KB to 5MB
		if err := cli.ConfigSet(ctx, "throttle-bytes-per-second", strconv.Itoa(randomInt)).Err(); err != nil {
			fmt.Println("Error setting key:", err)
			return
		}
	}
}

func UpdateTimout(cli *redis.Client, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := cli.ConfigSet(ctx, "throttle-bytes-per-second", "65535").Err(); err != nil {
		fmt.Println("Error setting key:", err)
		return
	}
	time.Sleep(time.Second * 3)
	rand.Seed(time.Now().UnixNano())
	for i := 1; i < 200; i++ {
		time.Sleep(time.Millisecond * 300)
		min := 20
		max := 200
		randomInt := rand.Intn(max-min+1) + min
		//do the update rsync-timeout-ms, randomly from 10 to 100ms
		if err := cli.ConfigSet(ctx, "rsync-timeout-ms", strconv.Itoa(randomInt)).Err(); err != nil {
			fmt.Println("Error setting key:", err)
			return
		}
	}
}

var _ = Describe("Rsync Reconfig Test", func() {
	ctx := context.TODO()
	var (
		slave1  *redis.Client
		slave2  *redis.Client
		master1 *redis.Client
	)

	BeforeEach(func() {
		slave1 = redis.NewClient(PikaOption(SLAVEADDR))
		slave2 = redis.NewClient(PikaOption(SLAVEADDR))
		master1 = redis.NewClient(PikaOption(MASTERADDR))
	})

	AfterEach(func() {
		Expect(slave1.Close()).NotTo(HaveOccurred())
		Expect(slave2.Close()).NotTo(HaveOccurred())
		Expect(master1.Close()).NotTo(HaveOccurred())
	})

	It("rsync reconfig rsync-timeout-ms, throttle-bytes-per-second", func() {
		slave1.SlaveOf(ctx, "no", "one")
		slave1.FlushDB(ctx)
		master1.FlushDB(ctx)
		time.Sleep(3 * time.Second)
		RefillMaster(MASTERADDR, 256, ctx)
		key1 := "45vs45f4s5d6"
		value1 := "afd54g5s4f545"
		//set key before sync happened, slave is supposed to fetch it when sync done
		err1 := master1.Set(ctx, key1, value1, 0).Err()
		Expect(err1).NotTo(HaveOccurred())

		//limit the rsync to prevent the sync finished before test finished
		err2 := slave1.ConfigSet(ctx, "throttle-bytes-per-second", "65535").Err()
		Expect(err2).NotTo(HaveOccurred())
		slave1.Do(ctx, "slaveof", "127.0.0.1", "9241", "force")
		time.Sleep(time.Second * 2)

		var wg sync.WaitGroup
		wg.Add(4)
		go UpdateThrottle(slave1, ctx, &wg)
		go UpdateTimout(slave1, ctx, &wg)
		go UpdateThrottle(slave2, ctx, &wg)
		go UpdateTimout(slave2, ctx, &wg)
		wg.Wait()

		ReleaseRsyncLimit(slave1, ctx)
		//full sync should be done after 20s due to rsync limit is removed
		time.Sleep(time.Second * 20)

		key2 := "rekaljfdkslj;"
		value2 := "ouifdhgisesdjkf"
		err3 := master1.Set(ctx, key2, value2, 0).Err()
		Expect(err3).NotTo(HaveOccurred())
		time.Sleep(time.Second * 5) //incr sync should also be done after 5s

		getValue1, err4 := slave1.Get(ctx, key1).Result()
		Expect(err4).NotTo(HaveOccurred())  //Get Slave failed after dynamic reset rsync rate and rsync timeout if err not nil
		Expect(getValue1).To(Equal(value1)) //Slave Get OK, but didn't fetch expected resp after dynamic reset rsync rate/timeout
		getValue2, err5 := slave1.Get(ctx, key2).Result()
		Expect(err5).NotTo(HaveOccurred())  //Get Slave failed after dynamic reset rsync rate and rsync timeout if err not nil
		Expect(getValue2).To(Equal(value2)) //Slave Get OK, but didn't fetch expected resp after dynamic reset rsync rate/timeout
	})

})
