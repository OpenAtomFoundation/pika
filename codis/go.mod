module pika/codis/v2

go 1.13

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4

require (
	github.com/BurntSushi/toml v1.2.1
	github.com/CodisLabs/codis v0.0.0-20181104082235-de1ad026e329
	github.com/codegangsta/inject v0.0.0-20150114235600-33e0aa1cb7c0 // indirect
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/etcd v3.3.27+incompatible
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/emirpasic/gods v1.18.1
	github.com/garyburd/redigo v1.6.4
	github.com/go-martini/martini v0.0.0-20170121215854-22fa46961aab
	github.com/influxdata/influxdb v1.11.0
	github.com/martini-contrib/binding v0.0.0-20160701174519-05d3e151b6cf
	github.com/martini-contrib/gzip v0.0.0-20151124214156-6c035326b43f
	github.com/martini-contrib/render v0.0.0-20150707142108-ec18f8345a11
	github.com/oxtoacart/bpool v0.0.0-20190530202638-03653db5a59c // indirect
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
	github.com/spinlock/jemalloc-go v0.0.0-20201010032256-e81523fb8524
	golang.org/x/net v0.6.0
	google.golang.org/grpc/examples v0.0.0-20230208220405-55dfae6e5bec // indirect
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
)
