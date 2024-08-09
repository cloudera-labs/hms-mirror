# Limits

Let's cover some practical limits of `hms-mirror` in the area of scale.

`hms-mirror` is designed to migrate databases.  While you can migrate 'every' databases in a schema using the `dbRegEx`
we don't recommend this.  You should construct a plan to migrate databases individually.

The architecture of `hms-mirror` does everything in memory.  Each table and partition consumes memory and contributes to
the load placed on systems we're using.

We've done practical tests to do a SCHEMA_ONLY migration of 10k tables between two clusters.  On our relatively
modest hardware, we accomplished this in about 8 minutes with the default memory settings.

Writing the reports, which happens at the end takes time.  It this case 1-2 minutes.

If you need to process more than what we've tested, please do a 'Dry-Run' first.  Watch the memory of the application
on the host where its running.  You may need to increase the memory profile of the application and try again. 
See [Memory Settings](Memory-Settings.md)

## Impact on Source and Target Systems

`hms-mirror` uses HiveServer2 connections to 'extract' and 'replay' schema's between systems.  It also utilizes Metastore
Direct connections to pull partition level details that can't be efficiently collected through the HiveServer2 SQL interface.

The concurrency (default of 10), will act like 10 users making a whole lot of DDL requests.  The impact to existing workloads
is a possibility.  If you have a large amount of data(metadata) to migrate/extract and you're concerned about the impact to
the user base, you should 'isolate' the HiveServer2 AND Hive Metastore for this process.  That could mean setting up an additional
HiveServer2/Metastore pair that is used specifically by 'hms-mirror'.

## Databases with a Large Number of Tables

As we mentioned, migrations are a database at a time.  If the table in the database exceed some of these limits,
consider using various `table filters` to process filtered lists of tables in the database at a time.

## Reports

Reports are created at the database level as well.  When the database has a large number of tables, these reports can be 
extremely long.
