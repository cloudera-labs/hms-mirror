# Strategies

A 'strategy' in `hms-mirror` is the method used to migrate metadata and possibly data from one metastore to another.

Some strategies are simply responsible for migrating metadata, while others are responsible for migrating metadata and data. 

## Data Movement

When the strategy is responsible for moving data as well, you have two options: `SQL` and `distcp`.  You can set this option here:

![dm_options.png](dm_options.png)

SQL will rely on the engine and the clusters ability to see across data storage environments to move the data.

For distcp [(Distributed Copy)](https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html), we'll build a plan that matches what you've asked to migrate.

For a matrix of when this might be available, see [Location Alignment](Location-Alignment.md).
