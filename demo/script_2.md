### Make Copy of DB
```
hms-mirror -d HYBRID -ma -ap -1 -db z_hms_mirror_testdb_20220720_195237 -dbp cp2_ -cfg .hms-mirror/cfg/default.yaml.self -o temp/cp2_z_hms_mirror_testdb_20220720_195237 -rdl -wd /warehouse/tablespace/managed/hive -ewd /warehouse/tablespace/external/hive -e --accept

hms-mirror -d HYBRID -ma -ap -1 -db z_hms_mirror_testdb_20220720_195237 -dbr demo -o temp/demo_cp -rdl -wd /warehouse/tablespace/managed/hive -ewd /warehouse/tablespace/external/hive -e --accept

hms-mirror -d HYBRID -ma -ap -1 -db demo -dbr demo_01 -cfg .hms-mirror/cfg/default.yaml.self -o temp/demo_01_cp -rdl -wd /warehouse/tablespace/managed/hive -ewd /warehouse/tablespace/external/hive -e --accept


```

```
EXPORT DB=cp1_z_hms_mirror_testdb_20220720_195237
export DB=cp2_z_hms_mirror_testdb_20220720_195237

```

### DRY_RUN
#### Relative Locations

```
hms-mirror -d STORAGE_MIGRATION -db ${DB} -o temp/${DB}_sm -smn ofs://OHOME90 -wd /warehouse/managed/hive -ewd /warehouse/external/hive -cfg .hms-mirror/cfg/default.yaml.cdp
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
hms-mirror -d STORAGE_MIGRATION -db ${DB} -o temp/${DB}_sm1 -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.cdp -rdl
```

##### ACID table Migration (part limit)

```
hms-mirror -d STORAGE_MIGRATION -ma -db ${DB} -o temp/${DB}_sm2 -smn ofs://OHOME90 -wd /warehouse/managed -ewd /warehouse/external -cfg .hms-mirror/cfg/default.yaml.cdp -rdl
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
hms-mirror -d DUMP -cfg .hms-mirror/cfg/default.yaml.self -db ${DB} -o temp/${DB}_dump_post_migration
```
