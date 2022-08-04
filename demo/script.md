# `hms-mirror` demo script

## Where to get

[Source](https://github.com/cloudera-labs/hms-mirror)
[Release](https://github.com/cloudera-labs/hms-mirror/releases)

## Fetch and Install

_as root_
```

cd /tmp
# remove only installs
rm -rf hms-mirror*
# fetch binary
wget https://github.com/cloudera-labs/hms-mirror/releases/download/1.5.2.0.7-SNAPSHOT/hms-mirror-dist.tar.gz
# untar
tar xzvf hms-mirror-dist.tar.gz
# install
hms-mirror-install/setup.sh
# DO NOT RUN from install dir.
```

## Review directory structures

- aux_libs (for kerberos)
- default.yaml (template) or --setup

_as user (not root)_

Get kerberos ticket.

```
cd
hms-mirror --help
```

## Review Environment

### HDFS / Ozone Volumes and Buckets

```
ozone sh vol list
ozone sh bucket list warehouse
ozone sh bucket list user
```

## Setup
Run scripts from $HOME

_in hive_

```
drop database if exists cp_merge_files cascade;
drop database if exists cp_tpcds_bin_partitioned_orc_2 cascade;
drop database if exists cp2_tpcds_bin_partitioned_orc_2 cascade;
```

_Copy merge_files and tpcds_bin_partitioned_orc_2 to working db's_

```
cd
# Simple DB Copy
hms-mirror -d HYBRID -ma -ap -1 -db merge_files -dbp cp_ -cfg .hms-mirror/cfg/default.yaml.self -o temp/cp_merge_files -rdl -wd /warehouse/tablespace/managed/hive -ewd /warehouse/tablespace/external/hive -e --accept
# tpcds DB Copy filter tables(limit)
hms-mirror -d HYBRID -ma -ap -1 -db tpcds_bin_partitioned_orc_2 -dbp cp_ -tf "web.*" -cfg .hms-mirror/cfg/default.yaml.self -o temp/cp_tpcds_bin_partitioned_orc_2 -e --accept
# tpcds DB Copy filter tables and downgrade acid
hms-mirror -d HYBRID -ma -da -ap -1 -db tpcds_bin_partitioned_orc_2 -dbp cp2_ -tf "web.*" -cfg .hms-mirror/cfg/default.yaml.self -o temp/cp2_tpcds_bin_partitioned_orc_2 -e --accept
```

## Review Databases: cp_merge_files, cp_tpcds_bin_partitioned_orc_2, and cp2_tpcds_bin_partitioned_orc_2

### From Hive

```
use cp_merge_files;
show tables;
select * from cp_merge_files.merge_files_part limit 10;
show create table merge_files_part;
use cp_tpcds_bin_partitioned_orc_2;
show tables;
select * from cp_tpcds_bin_partitioned_orc_2.web_sales limit 10;
show create table web_sales;
use cp2_tpcds_bin_partitioned_orc_2;
show tables;
select * from cp2_tpcds_bin_partitioned_orc_2.web_sales limit 10;
show create table web_sales;
```

### Dump of Schema

```
hms-mirror -d DUMP -cfg .hms-mirror/cfg/default.yaml.self -db cp_merge_files -o temp/cp_merge_files_dump
hms-mirror -d DUMP -cfg .hms-mirror/cfg/default.yaml.self -db cp_tpcds_bin_partitioned_orc_2 -o temp/cp_tpcds_bin_partitioned_orc_2_dump
hms-mirror -d DUMP -cfg .hms-mirror/cfg/default.yaml.self -db cp2_tpcds_bin_partitioned_orc_2 -o temp/cp2_tpcds_bin_partitioned_orc_2_dump
```

**Review Schemas in MD Viewer**


## Action

### DRY_RUN
#### Relative Locations

```
EXPORT DB=cp1_z_hms_mirror_testdb_20220720_195237
hms-mirror -d STORAGE_MIGRATION -db ${DB}
 -o temp/${DB}_sm -smn ofs://OHOME90 -wd /warehouse/managed/hive -ewd /warehouse/external/hive -cfg .hms-mirror/cfg/default.yaml.cdp
```

#### with distcp

```
hms-mirror -d STORAGE_MIGRATION -db ${DB} -dc -o temp/${DB}_dc_sm -smn ofs://OHOME90 -wd /warehouse/managed/hive -ewd /warehouse/external/hive -cfg .hms-mirror/cfg/default.yaml.cdp
```
#### Reset to default location

```
hms-mirror -d STORAGE_MIGRATION -db ${DB} -o temp/${DB}_sm_rdl -smn ofs://OHOME90 -wd /warehouse/managed/hive -ewd /warehouse/external/hive -cfg .hms-mirror/cfg/default.yaml.cdp -rdl
```

### WET_RUN

```
hms-mirror -d STORAGE_MIGRATION -db ${DB} -o temp/${DB}_sm_rdl -smn ofs://OHOME90 -wd /warehouse/managed/hive -ewd /warehouse/external/hive -cfg .hms-mirror/cfg/default.yaml.cdp -rdl -e --accept
```

#### Review Database

```
hms-mirror -d DUMP -cfg .hms-mirror/cfg/default.yaml.self -db ${DB} -o temp/${DB}_dump_post_migration
```

### ACID Migrations

#### DRY_RUN

##### ACID table Migration (missing -ma)

```
hms-mirror -d STORAGE_MIGRATION -db cp_tpcds_bin_partitioned_orc_2 -o temp/cp_tpcds_2_sm1 -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.cdp -rdl
```

##### ACID table Migration (part limit)

```
hms-mirror -d STORAGE_MIGRATION -ma -db cp_tpcds_bin_partitioned_orc_2 -o temp/cp_tpcds_2_sm2 -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.cdp -rdl
```

##### ACID table Migration 

```
hms-mirror -d STORAGE_MIGRATION -ma -ap -1 -db ${DB} -o temp/${DB}_all_sm -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.cdp -rdl
```

#### WET_RUN

```
hms-mirror -d STORAGE_MIGRATION -ma -ap -1 -db ${DB} -o temp/${DB}_all_wr_sm -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.cdp -rdl -e --accept
```

```
hms-mirror -d DUMP -cfg .hms-mirror/cfg/default.yaml.self -db cp_tpcds_bin_partitioned_orc_2 -o temp/cp_tpcds_bin_partitioned_orc_2_dump_post_migration
```

```
use cp_tpcds_bin_partitioned_orc_2;
show tables;
```

### External Migrations
#### DRY_RUN

#####  External Migration

```
hms-mirror -d STORAGE_MIGRATION -db cp2_tpcds_bin_partitioned_orc_2 -o temp/cp2_tpcds_2_sm1 -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.self -rdl
```

##### External Migration (part limit)

```
hms-mirror -d STORAGE_MIGRATION -db cp2_tpcds_bin_partitioned_orc_2 -o temp/cp2_tpcds_2_sm2 -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.self -rdl
```

##### Migration 

```
hms-mirror -d STORAGE_MIGRATION -sp -1 -db cp2_tpcds_bin_partitioned_orc_2 -o temp/cp2_tpcds_2_sm3 -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.self -rdl
```

#### WET_RUN

```
hms-mirror -d STORAGE_MIGRATION -sp -1 -db cp2_tpcds_bin_partitioned_orc_2 -o temp/cp2_tpcds_2_sm_wet -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.self -rdl -e --accept
```

```
hms-mirror -d DUMP -cfg .hms-mirror/cfg/default.yaml.self -db cp2_tpcds_bin_partitioned_orc_2 -o temp/cp2_tpcds_2_dump_post_migration
```

```
use cp2_tpcds_bin_partitioned_orc_2;
show tables;
```
