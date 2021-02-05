## Shared Authentication

The clusters must share a common authentication model to support cross cluster HDFS access, when HDFS is the underlying storage layer for the datasets.  This means that a **kerberos** ticket used in the RIGHT cluster must be valid for the LEFT cluster.

For cloud storage, the two clusters must have rights to the target storage bucket.

It you are able to [`distcp` between the clusters](./link_clusters.md), then you have the basic connectivity required to start working with `hms-mirror`.