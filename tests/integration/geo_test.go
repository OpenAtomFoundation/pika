package pika_integration

import (
	"context"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Geo Commands", func() {
	ctx := context.TODO()
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(PikaOption(SINGLEADDR))
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		if GlobalBefore != nil {
			GlobalBefore(ctx, client)
		}
		time.Sleep(1 * time.Second)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("Geo add and radius search", func() {
		BeforeEach(func() {
			n, err := client.GeoAdd(
				ctx,
				"Sicily",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "Palermo"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "Catania"},
			).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should not add same geo location", func() {
			geoAdd := client.GeoAdd(
				ctx,
				"Sicily",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "Palermo"},
			)
			Expect(geoAdd.Err()).NotTo(HaveOccurred())
			Expect(geoAdd.Val()).To(Equal(int64(0)))
		})

		It("should search geo radius", func() {
			res := client.Do(ctx, "GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHDIST", "WITHCOORD")
			Expect(res.Err()).NotTo(HaveOccurred())
			Expect(res.Val()).To(HaveLen(2))

			Expect(res.Val()).To(Equal([]interface{}{[]interface{}{"Catania", "56.4413", []interface{}{"15.087267458438873", "37.50266842333162"}}, []interface{}{"Palermo", "190.4424", []interface{}{"13.361389338970184", "38.115556395496299"}}}))

		})

		It("should geo radius and store the result", func() {
			n, err := client.GeoRadiusStore(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
				Radius: 200,
				Store:  "result",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			res, err := client.ZRangeWithScores(ctx, "result", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainElement(redis.Z{
				Score:  3.479099956230698e+15,
				Member: "Palermo",
			}))
			Expect(res).To(ContainElement(redis.Z{
				Score:  3.479447370796909e+15,
				Member: "Catania",
			}))
		})

		It("should geo radius and store dist", func() {
			n, err := client.GeoRadiusStore(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
				Radius:    200,
				StoreDist: "result",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			res, err := client.ZRangeWithScores(ctx, "result", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainElement(redis.Z{
				Score:  190.44242984775784,
				Member: "Palermo",
			}))
			Expect(res).To(ContainElement(redis.Z{
				Score:  56.4412578701582,
				Member: "Catania",
			}))
		})

		//It("should search geo radius with options", func() {
		//	res, err := client.GeoRadius(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
		//		Radius:      200,
		//		Unit:        "km",
		//		WithGeoHash: true,
		//		WithCoord:   true,
		//		WithDist:    true,
		//		Count:       2,
		//		Sort:        "ASC",
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(res).To(HaveLen(2))
		//	Expect(res[1].Name).To(Equal("Palermo"))
		//	Expect(res[1].Dist).To(Equal(190.4424))
		//	Expect(res[1].GeoHash).To(Equal(int64(3479099956230698)))
		//	Expect(res[1].Longitude).To(Equal(13.361389338970184))
		//	Expect(res[1].Latitude).To(Equal(38.115556395496299))
		//	Expect(res[0].Name).To(Equal("Catania"))
		//	Expect(res[0].Dist).To(Equal(56.4413))
		//	Expect(res[0].GeoHash).To(Equal(int64(3479447370796909)))
		//	Expect(res[0].Longitude).To(Equal(15.087267458438873))
		//	Expect(res[0].Latitude).To(Equal(37.50266842333162))
		//})

		//It("should search geo radius with WithDist=false", func() {
		//	res, err := client.GeoRadius(ctx, "Sicily", 15, 37, &redis.GeoRadiusQuery{
		//		Radius:      200,
		//		Unit:        "km",
		//		WithGeoHash: true,
		//		WithCoord:   true,
		//		Count:       2,
		//		Sort:        "ASC",
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(res).To(HaveLen(2))
		//	Expect(res[1].Name).To(Equal("Palermo"))
		//	Expect(res[1].Dist).To(Equal(float64(0)))
		//	Expect(res[1].GeoHash).To(Equal(int64(3479099956230698)))
		//	Expect(res[1].Longitude).To(Equal(13.361389338970184))
		//	Expect(res[1].Latitude).To(Equal(38.115556395496299))
		//	Expect(res[0].Name).To(Equal("Catania"))
		//	Expect(res[0].Dist).To(Equal(float64(0)))
		//	Expect(res[0].GeoHash).To(Equal(int64(3479447370796909)))
		//	Expect(res[0].Longitude).To(Equal(15.087267458438873))
		//	Expect(res[0].Latitude).To(Equal(37.50266842333162))
		//})

		//It("should search geo radius by member with options", func() {
		//	res, err := client.GeoRadiusByMember(ctx, "Sicily", "Catania", &redis.GeoRadiusQuery{
		//		Radius:      200,
		//		Unit:        "km",
		//		WithGeoHash: true,
		//		WithCoord:   true,
		//		WithDist:    true,
		//		Count:       2,
		//		Sort:        "ASC",
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(res).To(HaveLen(2))
		//	Expect(res[0].Name).To(Equal("Catania"))
		//	Expect(res[0].Dist).To(Equal(0.0))
		//	Expect(res[0].GeoHash).To(Equal(int64(3479447370796909)))
		//	Expect(res[0].Longitude).To(Equal(15.087267458438873))
		//	Expect(res[0].Latitude).To(Equal(37.50266842333162))
		//	Expect(res[1].Name).To(Equal("Palermo"))
		//	Expect(res[1].Dist).To(Equal(166.2742))
		//	Expect(res[1].GeoHash).To(Equal(int64(3479099956230698)))
		//	Expect(res[1].Longitude).To(Equal(13.361389338970184))
		//	Expect(res[1].Latitude).To(Equal(38.115556395496299))
		//})

		//It("should search geo radius with no results", func() {
		//	res, err := client.GeoRadius(ctx, "Sicily", 99, 37, &redis.GeoRadiusQuery{
		//		Radius:      200,
		//		Unit:        "km",
		//		WithGeoHash: true,
		//		WithCoord:   true,
		//		WithDist:    true,
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(res).To(HaveLen(0))
		//})

		It("should get geo distance with unit options", func() {
			// From Redis CLI, note the difference in rounding in m vs
			// km on Redis itself.
			//
			// GEOADD Sicily 13.361389 38.115556 "Palermo" 15.087269 37.502669 "Catania"
			// GEODIST Sicily Palermo Catania m
			// "166274.15156960033"
			// GEODIST Sicily Palermo Catania km
			// "166.27415156960032"
			dist, err := client.GeoDist(ctx, "Sicily", "Palermo", "Catania", "km").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(dist).To(BeNumerically("~", 166.27, 0.01))

			dist, err = client.GeoDist(ctx, "Sicily", "Palermo", "Catania", "m").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(dist).To(BeNumerically("~", 166274.15, 0.01))
		})

		//It("should get geo hash in string representation", func() {
		//	hashes, err := client.GeoHash(ctx, "Sicily", "Palermo", "Catania").Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(hashes).To(ConsistOf([]string{"sqc8b49rny0", "sqdtr74hyu0"}))
		//})

		It("should return geo position", func() {
			pos, err := client.GeoPos(ctx, "Sicily", "Palermo", "Catania", "NonExisting").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(pos).To(ConsistOf([]*redis.GeoPos{
				{
					Longitude: 13.361389338970184,
					Latitude:  38.1155563954963,
				},
				{
					Longitude: 15.087267458438873,
					Latitude:  37.50266842333162,
				},
				nil,
			}))
		})

		//It("should geo search", func() {
		//	q := &redis.GeoSearchQuery{
		//		Member:    "Catania",
		//		BoxWidth:  400,
		//		BoxHeight: 100,
		//		BoxUnit:   "km",
		//		Sort:      "asc",
		//	}
		//	val, err := client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.BoxHeight = 400
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania", "Palermo"}))
		//
		//	q.Count = 1
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.CountAny = true
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Palermo"}))
		//
		//	q = &redis.GeoSearchQuery{
		//		Member:     "Catania",
		//		Radius:     100,
		//		RadiusUnit: "km",
		//		Sort:       "asc",
		//	}
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.Radius = 400
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania", "Palermo"}))
		//
		//	q.Count = 1
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.CountAny = true
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Palermo"}))
		//
		//	q = &redis.GeoSearchQuery{
		//		Longitude: 15,
		//		Latitude:  37,
		//		BoxWidth:  200,
		//		BoxHeight: 200,
		//		BoxUnit:   "km",
		//		Sort:      "asc",
		//	}
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.BoxWidth, q.BoxHeight = 400, 400
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania", "Palermo"}))
		//
		//	q.Count = 1
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.CountAny = true
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Palermo"}))
		//
		//	q = &redis.GeoSearchQuery{
		//		Longitude:  15,
		//		Latitude:   37,
		//		Radius:     100,
		//		RadiusUnit: "km",
		//		Sort:       "asc",
		//	}
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.Radius = 200
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania", "Palermo"}))
		//
		//	q.Count = 1
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Catania"}))
		//
		//	q.CountAny = true
		//	val, err = client.GeoSearch(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]string{"Palermo"}))
		//})
		//
		//It("should geo search with options", func() {
		//	q := &redis.GeoSearchLocationQuery{
		//		GeoSearchQuery: redis.GeoSearchQuery{
		//			Longitude:  15,
		//			Latitude:   37,
		//			Radius:     200,
		//			RadiusUnit: "km",
		//			Sort:       "asc",
		//		},
		//		WithHash:  true,
		//		WithDist:  true,
		//		WithCoord: true,
		//	}
		//	val, err := client.GeoSearchLocation(ctx, "Sicily", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal([]redis.GeoLocation{
		//		{
		//			Name:      "Catania",
		//			Longitude: 15.08726745843887329,
		//			Latitude:  37.50266842333162032,
		//			Dist:      56.4413,
		//			GeoHash:   3479447370796909,
		//		},
		//		{
		//			Name:      "Palermo",
		//			Longitude: 13.36138933897018433,
		//			Latitude:  38.11555639549629859,
		//			Dist:      190.4424,
		//			GeoHash:   3479099956230698,
		//		},
		//	}))
		//})

		//It("should geo search store", func() {
		//	q := &redis.GeoSearchStoreQuery{
		//		GeoSearchQuery: redis.GeoSearchQuery{
		//			Longitude:  15,
		//			Latitude:   37,
		//			Radius:     200,
		//			RadiusUnit: "km",
		//			Sort:       "asc",
		//		},
		//		StoreDist: false,
		//	}
		//
		//	val, err := client.GeoSearchStore(ctx, "Sicily", "key1", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal(int64(2)))
		//
		//	q.StoreDist = true
		//	val, err = client.GeoSearchStore(ctx, "Sicily", "key2", q).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(val).To(Equal(int64(2)))
		//
		//	loc, err := client.GeoSearchLocation(ctx, "key1", &redis.GeoSearchLocationQuery{
		//		GeoSearchQuery: q.GeoSearchQuery,
		//		WithCoord:      true,
		//		WithDist:       true,
		//		WithHash:       true,
		//	}).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(loc).To(Equal([]redis.GeoLocation{
		//		{
		//			Name:      "Catania",
		//			Longitude: 15.08726745843887329,
		//			Latitude:  37.50266842333162032,
		//			Dist:      56.4413,
		//			GeoHash:   3479447370796909,
		//		},
		//		{
		//			Name:      "Palermo",
		//			Longitude: 13.36138933897018433,
		//			Latitude:  38.11555639549629859,
		//			Dist:      190.4424,
		//			GeoHash:   3479099956230698,
		//		},
		//	}))
		//
		//	v, err := client.ZRangeWithScores(ctx, "key2", 0, -1).Result()
		//	Expect(err).NotTo(HaveOccurred())
		//	Expect(v).To(Equal([]redis.Z{
		//		{
		//			Score:  56.441257870158204,
		//			Member: "Catania",
		//		},
		//		{
		//			Score:  190.44242984775784,
		//			Member: "Palermo",
		//		},
		//	}))
		//})
	})
})
