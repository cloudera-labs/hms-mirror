# LINKED

Assumes the clusters are [linked](Linking-Cluster-Storage-Layers.md).  We'll transfer the schema and leave the location as is on the new cluster.

This provides a means to test hive on the RIGHT cluster using the LEFT cluster's storage.

The `-ma` (migrate acid) tables option is NOT valid in this scenario and will result in an error if specified.


WARNING:  If the LOCATION element is specified in the database definition AND you use `DROP DATABASE ... CASCADE` from the RIGHT cluster, YOU WILL DROP THE DATA ON THE LEFT CLUSTER even though the tables are NOT purgeable.  This is the DEFAULT behavior of hive 'DROP DATABASE'.  So BE CAREFUL!!!!
