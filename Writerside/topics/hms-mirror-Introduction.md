# Introduction

<tooltip term="hms-mirror">"hms-mirror"</tooltip> is a utility used to bridge the gap between two clusters and migrate `hive` _metadata_.  HMS-Mirror is distributed under the [APLv2](license.md) license.

The application will migrate hive metastore data (metadata) between two clusters.  With [SQL](SQL.md) and [EXPORT_IMPORT](EXPORT_IMPORT.md) data strategies, we can move data between the two clusters using Hive SQL.

As an alternative to using Hive SQL to move data between two clusters, `hms-mirror` can build `distcp` plans and scripts that can be run in concert with the metadata scripts to complete the migration.

You start by picking a 'Data Strategy' that fits your needs.  A **Data Strategy** defines how you're choosing to migrate 'metadata' and 'data' between two clusters.  Some of the strategies are 'metadata' only, and some are 'metadata' and 'data'.  Others are used to migrate data 'within' a cluster (STORAGE_MIGRATION) to facilitate changes in the storage layer.  This can be to move data into an encrypted zone or to a different storage layer, like Ozone.

There are two interface (working on a third) for `hms-mirror`: [CLI](CLI-Interface.md) and [WebUI](Web-Interface.md)

From both interfaces, reports are generated that detail the actions taken by the application.  You can direct it to run the conversion scripts automatically or just generate the scripts for you to run later.
