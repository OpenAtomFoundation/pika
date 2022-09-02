## Since Pika v3.1.2, we support sharding and add some commands.

At this version, we have a new object named 'table' which contains several slots(max count depend on default-slot-num). Every key in request will map to one slot in one table. 

If running in sharding mode, Pika will create a table named db0 as default and create meta files in db-path.

Data sync between slots in different instance. Slave slot is readonly.

### 1.`pkcluster info`：
Usage: print slots sync information

`pkcluster info slot`: print slot information in default table

`pkcluster info slot db0:0-6,7,8`: print slot 0-6,7,8 in table0

`pkcluster info table 1`: print information in table1 include QPS, sharding.


### 2.`pkcluster addslots`：

Usage: add alot in default table, range [0，default-slot-num - 1]

`pkcluster addslots 0-2`: add slot 0,1,2 in default table

`pkcluster addslots 0-2,3`: add slot 0,1,2,3 in default table

`pkcluster addslots 0,1,2,3,4`: add slot 0,1,2,3,4 in default table


### 3.`pkcluster delslots`：

Usage :delete alot in default table, range [0，default-slot-num - 1]

`pkcluster delslots 0-2`: delete slot 0,1,2 in default table

`pkcluster delslots 0-2,3`: add slot 0,1,2,3 in default table

`pkcluster delslots 0,1,2,3,4`: add slot 0,1,2,3,4 in default table


### 4.`pkcluster slotsslaveof`：

Usage:trigger data sync from master slot to slave slot in different Pika instances.

`pkcluster slotsslaveof no one  [0-3,8-11 | all]` disconnect data sync in slots 

`pkcluster slotsslaveof ip port [0-3,8,9,10,11 | all]` connect data sync in slots 

`pkcluster slotsslaveof ip port [0,2,4,6,7,8,9 | all] force` trigger full sync in slots 

### 5.`slaveof` 

same as `pkcluster slotsslave ip port all` 

## From v3.3, Pika can create table dynamicly. It will create table 0, slot num can be config. If need other tables should create manually.
### 1. `pkcluster addtable` ：

Usage: create table, need set table-id and max-slot-num, a as default.

`pkcluster addtable 1 64`:create table, table-id is 1 and max-slot-num is 64.

### 2. `pkcluster deltalbe` ：

Usage: delete table and it's slots 

` pkcluster deltable 1` : delete table 1 and all it's slots

### 3.`pkcluster addslots`：

Usage:add slot in table, slots range [0，max-slot-num - 1]

`pkcluster addslots 0-2 1`: add slot 0,1,2 in table 1 

`pkcluster addslots 0-2,3 1`: add slot 0,1,2,3 in table 1

`pkcluster addslots 0,1,2,3,4 1`: add slot 0,1,2,3,4 in table 1

### 4.`pkcluster delslots`：

Usage:delete slot in table, range [0，max-slot-num - 1]

`pkcluster delslots 0-2 1`: delete slot 0,1,2 in table 1 

`pkcluster delslots 0-2,3 1`: delete slot 0,1,2,3 in table 1 

`pkcluster delslots 0,1,2,3,4 1`: delete slot 0,1,2,3,4 in table 1 

### 5.`pkcluster slotsslaveof`：

Usage:trigger data sync from master slot to slave slot in different Pika instances.

`pkcluster slotsslaveof no one  [0-3,8-11 | all] 1 ` disconnect data sync in slots

`pkcluster slotsslaveof ip port [0-3,8,9,10,11 | all] 1` connect data sync in slots 

`pkcluster slotsslaveof ip port [0,2,4,6,7,8,9 | all] force 1` trigger full sync in slots 


<br/>

## Notice：
### In sharding mode, single key command is all supported. 
### Multiple keys command are partially supported
<br/>

#### Not supported

| —           | —                 | —            |
| ----------- | ----------------- | ------------ |
| Msetnx      | Scan              | Keys         |
| Scanx       | PKScanRange       | PKRScanRange |
| RPopLPush   | ZUnionstore       | ZInterstore  |
| SUnion      | SUnionstore       | SInter       |
| SInterstore | SDiff             | SDiffstore   |
| SMove       | BitOp             | PfAdd        |
| PfCount     | PfMerge           | GeoAdd       |
| GeoPos      | GeoDist           | GeoHash      |
| GeoRadius   | GeoRadiusByMember |              |

#### Command Support Plan in Sharding Mode

For multiple keys commands, if all keys in one slot will be supported. Globle distribution command will not be supported. 
