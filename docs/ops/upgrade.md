
# How to upgrade to Pika3.0


## Prepare:  
Pika v2.3.3 can not upgrade to Pika v2.3.3~Pika v2.3.6 
* If your version < v2.3.3, you need prepare new version >= v3.0.16
* If your version >=v2.3.3, you need prepare new version >= v3.0.16
* If your version >=v2.3.3, you need start from step 5, else start from step 1
* Please backup your Pika before upgrade 
## Steps:
1. Add 'masterauth' configure same with master 'requirepass'
2. Replace old Pika bin path with new 
3. Restart Pika master and slave 
4. Check is master-slave up 
5. Deploy Pika v3.0.16
6. In slave node run bgsave, make sure info file in dump path exist and backup it
7. Run nemo_to_blackwidow tool in Pika v3.0.16  
```
./nemo_to_blackwidow nemo_db_path blackwidow_db_path -n

```
8. Update configure files, new path(/data/pika-new30/new_db) as start up path, modify identify-binlog-type as old
9. Stop slave in server(/data/pika-demo), startup Pika v3.0.16(/data/pika-new30/new_db)
10. Login Pika v3.0.16 and connect master/slave. Open the backup info file and copy the offset information to start partical sync.
```
Example: in info file ip:192.168.1.1 port:6666
3s
192.168.1.2
6666
300
17055479
slaveof should:
slaveof 192.168.1.1 6666 300 17055479
```
11. Check is sync status up
```
a.Close slave node configure 'salve-read-only'
b.Redirect request to slave node 
c.Disconnect slave node
```
12. Run config set 'identify-binlog-type' as new
13. Complete

## Tips:
* Version > v3.0 Pika refactor the store data structure, you will find the storage size become smaller. 
* Make sure binlog expire time is long enough(expire-logs-days and expire-logs-nums) before use nemo_to_blackwidow tool. 
* After Pika v3.0, we refactor the binlog format, so we provide 'identify-binlog-type' configure which affect in in slaves, if configure 'new' it will parse binlog in new format, if configure 'old' it will parse binlog in old format(pika2.3.3 ~ pika2.3.6). 
* Compare binlog between 'old' and 'new' format you can see 'lag' in the master, no matter the binlog_offset 


# How to upgrade to Pika v3.1 or v3.2

# Tools
## manifest generation
* Path: `./tools/manifest_generator`
* Usage: generate manifest
## partial sync tool
* Path`./tools/pika_port`
* Usage: data sync between Pika v3.0 and Pika v3.1/2

# Announce
1. Performance reason, since Pika v3.1, we support multiple DB mode, so, db and log store path changed, in old version Pika, DB path is `/data/pika9221/db`, in multiple DB mode, it is `/data/pika9221/db/db0`
2. Performance reason, we use PB protocol in communication between instances, so old version instance and new version cannot talk. [pika_port](https%3a%2f%2fgithub.com%2fQihoo360%2fpika%2fwiki%2fpika%e5%88%b0pika%e3%80%81redis%e8%bf%81%e7%a7%bb%e5%b7%a5%e5%85%b7) Old version Pika sync to new version Pika

# Steps
1. Configures
2. Master bgsave, copy dump files to new version db-path/db0 
```
Example:
    Old version Pika dump：/data/pika_old/dump/20190517/
    New version Pika db-path：/data/pika_new/db/
    Run: cp -r /data/pika_old/dump/20190517/ /data/pika_new/db/db0/
```
3. Run manifest_generator tool in new version Pika log-path/log_db0 to generate manifest file
```
Example:
    New version Pika db-path: /data/pika_new/db/
    New version Pika log-path: /data/pika_new/log/
    Rum: ./manifest_generator -d /data/pika_new/db/db0 -l /data/pika_new/log/log_db0
```
4. Startup Pika v3.1.0, run info log show binlog(filenum and offset)
5. Run Pika-port tool old version Pika data
```
Example:
    Old version Pika: 192.168.1.1:9221
    New version Pika: 192.168.1.2:9222
    Run pika_port server: 192.168.1.3, use port:9223
    filenum:100, offset:999

  Run ./pika_port -t 192.168.1.3 -p 9223 -i 192.168.1.1 -o 9221 -m 192.168.1.2 -n 9222 -f 100 -s 999 -e
```
6. Run `info replication` in master can print information of pika_port tool

7. With partial sync, we can add slaves. When lag nearly to 0, switch write and replace old Pika.

