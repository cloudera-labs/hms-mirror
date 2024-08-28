# Storage Systems (Distributed File Systems)

By default, `hms-mirror` is built with native support to communicate with 'hdfs', and 'ozone' distributed file systems. The `hms-mirror` can be extended to support other distributed file systems by implementing an `hcfs`(hadoop compatible filesystem) interface.  These include Amazon S3, Google Cloud Storage, Azure Blob Storage, etc.

To support communication with any of these cloud platforms or other distributed files systems, you'll need to ensure the libraries needed to support that communication are available in the classpath for 'hms-mirror'.

Make sure these are available in the classpath for `hms-mirror` **BEFORE** you start the application (Web and CLI).

We'll cover the libraries in the following sections.  These libraries need to be copied to the `$HOME/.
hms-mirror/aux_libs` directory.

All these libraries are available in the Cloudera distribution.  The below file listings have been sourced through 
our community testing and may not be exhaustive.  If you find we've missed any, please let us know by logging an 
issue at [hms-mirror Github Issues](https://github.com/cloudera-labs/hms-mirror/issues)

## Amazon S3

This obviously includes Amazon S3, but also includes other S3 compatible storage systems like Minio, etc. that are compatible with the S3 API.

```
hadoop-aws-<platform-version>.jar
aws-java-sdk-bundle-<platform-version>.jar
ranger-raz-hook-s3-<platform-version>.jar
```

## Microsoft Azure

**abfs**

```
hadoop-azure-<platform-version>9.jar
ranger-raz-hook-abfs-<platform-version>.jar
```

## Google Cloud Storage (GFS)

```
google-cloud-storage-<platform-version>.jar
```