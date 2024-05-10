module pika/codis/v2

go 1.19

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4

replace google.golang.org/grpc => google.golang.org/grpc v1.29.0

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/docopt/docopt-go v0.0.0-20180111231733-ee0de3bc6815
	github.com/emirpasic/gods v1.18.1
	github.com/garyburd/redigo v1.6.4
	github.com/go-martini/martini v0.0.0-20170121215854-22fa46961aab
	github.com/influxdata/influxdb v1.11.0
	github.com/martini-contrib/binding v0.0.0-20160701174519-05d3e151b6cf
	github.com/martini-contrib/gzip v0.0.0-20151124214156-6c035326b43f
	github.com/martini-contrib/render v0.0.0-20150707142108-ec18f8345a11
	github.com/samuel/go-zookeeper v0.0.0-20201211165307-7117e9ea2414
	github.com/spinlock/jemalloc-go v0.0.0-20201010032256-e81523fb8524
	github.com/stretchr/testify v1.8.0
	go.etcd.io/etcd/client/v2 v2.305.7
	golang.org/x/net v0.23.0
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
)

require (
	github.com/codegangsta/inject v0.0.0-20150114235600-33e0aa1cb7c0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/oxtoacart/bpool v0.0.0-20190530202638-03653db5a59c // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.7 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
