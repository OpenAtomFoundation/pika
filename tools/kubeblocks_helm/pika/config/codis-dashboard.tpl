
##################################################
#                                                #
#                  Codis-Dashboard               #
#                                                #
##################################################

# Set Coordinator, only accept "zookeeper" & "etcd" & "filesystem".
# for zookeeper/etcd, coorinator_auth accept "user:password" 
# Quick Start
coordinator_name = "filesystem"
coordinator_addr = "/tmp/codis"
coordinator_name = "etcd"
coordinator_addr = "pika-cluster-etcd-0.pika-cluster-etcd-headless:2379,pika-cluster-etcd-1.pika-cluster-etcd-headless:2379,pika-cluster-etcd-1.pika-cluster-etcd-headless:2379"
#coordinator_auth = ""

# Set Codis Product Name/Auth.
product_name = "codis-demo"
product_auth = ""

# Set bind address for admin(rpc), tcp only.
admin_addr = "0.0.0.0:18080"

# Set slot num
max_slot_num = 1024

# Set arguments for data migration (only accept 'sync' & 'semi-async').
migration_method = "semi-async"
migration_parallel_slots = 100
migration_async_maxbulks = 200
migration_async_maxbytes = "32mb"
migration_async_numkeys = 500
migration_timeout = "30s"

# Set configs for redis sentinel.
sentinel_check_server_state_interval = "10s"
sentinel_check_master_failover_interval = "2s"
sentinel_master_dead_check_times = 10
sentinel_check_offline_server_interval = "2s"
sentinel_client_timeout = "10s"
sentinel_quorum = 2
sentinel_parallel_syncs = 1
sentinel_down_after = "30s"
sentinel_failover_timeout = "5m"
sentinel_notification_script = ""
sentinel_client_reconfig_script = ""

