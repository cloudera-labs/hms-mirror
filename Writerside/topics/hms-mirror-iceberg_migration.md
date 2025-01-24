# ICEBERG_MIGRATION

This process will look at Hive tables, evaluate if the table is a candidate for migration to Iceberg, and then migrate the table to Iceberg.

Currently the conversion of Hive Table Format to Iceberg is supported through the [STORAGE_MIGRATION](storage_migration.md) data strategy.  Efforts are in place to extend this conversion to SCHEMA_ONLY and SQL data strategies.

![iceberg_conversion.png](iceberg_conversion.png)

Simply 'enable' the conversion and all tables covered by the data strategy will be converted to Iceberg.  You can 
also determine which iceberg table format to apply to the table.

## Requirements

- Requires Hive with Iceberg Support.
  - CDP Private Cloud Base 7.1.9 Hive does NOT support Iceberg.
  - CDP Private Cloud CDW 1.5.1 Hive does support Iceberg.  You need CDW Data Services 1.5.1 or higher.
  - CDP Public Cloud Hive does support Iceberg as of August 2023 (Datahub and CDW)

## Caution

Make sure you know the component limitations with Iceberg Tables [here](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html) so you aren't caught by surprise.
