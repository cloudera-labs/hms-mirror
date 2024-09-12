# STORAGE_MIGRATION

The Storage Migration feature in `hms-mirror` is used to migrate 'data' from one storage location to another in the 
same metastore.

For example:  You have all of you databases data on 'HDFS' and you want to more it to 'Ozone' while keeping all the 
same details about the tables, columns, etc. in the metastore so there's minimal impact to the applications using 
the database/tables.

One of the tricky parts of this type of migration is how you'll want to handle going from a 'single' namespace 
environment to a 'multi' namespace environment.  This is the case when migrating from HDFS to Ozone.  Ozone features,
and multi-tenant capabilities are different than HDFS.  On HDFS, it's ok to have all you databases in a single 
parent folder (Warehouse Directory).  But with Ozone, you do NOT want to do that.  This isn't necessarily an 'Ozone' 
only adjustment.  This applies to cloud storage (S3, ADLS, GFS) too.

In this case, you'll want to organize your databases into separate 'namespaces' on the storage system.  We're using 
the term 'namespace' loosely here.  It could be a 'bucket' in S3, a 'container' in ADLS, or a 'volume/bucket' in 
Ozone.  Regardless, the point is that we need to be able to handle each database as a separate entity in the storage 
migration process and allow for each database to be migrated to a different 'namespace' in the target storage system.

Let's take a look at the following example in table format.

| Group   | Database Name    | HDFS<br/>External Location | HDFS<br/>Managed Location | Ozone<br/>External Location | Ozone<br/>Managed Location |
|---------|------------------|--------------------------------------------|-------------------------------------------|-----------------------------|----------------------------|
| finance | db1              | /warehouse/tablespace/hive/external/db1.db | /warehouse/tablespace/hive/managed/db1.db | /finance/external/db1.db    | /finance/managed/db1.db    |
| finance | db2              | /warehouse/tablespace/hive/external/db2.db | /warehouse/tablespace/hive/managed/db2.db | /finance/external/db2.db    | /finance/managed/db2.db    |
| hr      | db3              | /warehouse/tablespace/hive/external/db3.db | /warehouse/tablespace/hive/managed/db3.db | /hr/external/db3.db         | /hr/managed/db3.db         |
| hr      | db4              | /warehouse/tablespace/hive/external/db4.db | /warehouse/tablespace/hive/managed/db4.db | /hr/external/db4.db         | /hr/managed/db4.db         |

In the above example, we have 2 owners (finance, hr) and 4 databases (db1, db2, db3, db4).  In the legacy 
environment (HDFS), all databases are in the same parent folder.  In the target environment (Ozone), we want to take 
advantage of the multi-tenant capabilities and separate the databases into separate 'namespaces'.  This is done by 
creating different 'volume/bucket' areas for each 'Owner' and then placing the databases in those areas.  To further 
illustrate separation, look how we established the 'external' and 'managed' locations for each database.

`hms-mirror` helps you build [Warehouse Plans](#warehouse-plans) to handle these types of migrations.  Each database 
to migrate will have a 'Warehouse Plan' that will define how to handle the migration.

## Important Properties

The following screen shot shows the properties that are important for the Storage Migration feature.

![sm_dm_screen.png](sm_dm_screen.png)

**Target Namespace**

Identifies the 'namespace' where all the translations will be made to.

**Location Translation Strategy**

RELATIVE or ALIGNED.  In this case, we DO want to ALIGN the locations with the new 'namespace' and locations defined 
in the Warehouse Plan for the database.

**Movement Strategy**

DISTCP or SQL.  This will direct `hms-mirror` to build a `distcp` plan/scripts or construct `SQL` to move the data.

## Demo

Here's how the Warehouse Plans would look for the above example:

![warehouseplans_01.png](warehouseplans_01.png)

Here's one of the report details for the 'finance.db1' database.  See how the 'external' and 'managed' locations are 
have been altered to reflect the new 'namespace' structure in Ozone we requested in the Warehouse Plan.

![warehouseplan_01_rpt.png](warehouseplan_01_rpt.png)

