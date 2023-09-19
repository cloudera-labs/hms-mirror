## Iceberg Migration

This process will look at Hive tables, evaluate if the table is a candidate for migration to Iceberg, and then migrate the table to Iceberg.

Specify `-d ICEBERG_CONVERSION` as the DataStrategy to run the Iceberg Migration.

This process uses Hive SQL to build(and run) the conversion scripts.  These are "inplace" migrations.

The following options apply to the Iceberg Migration:
- `-iv|--iceberg-version` - The Iceberg version to use.  Default is 1.  Can be 1 or 2.
- `-itpo|--iceberg-table-property-overrides` - A comma separated list of table properties to override.  See the Iceberg Table Properties available [here](https://iceberg.apache.org/docs/latest/configuration/#configuration) and [CDP Properties](https://docs.cloudera.com/cdw-runtime/cloud/iceberg-how-to/topics/iceberg-table-properties.html)

### Requirements

- Requires Hive with Iceberg Support.
  - CDP Private Cloud Base 7.1.9 Hive does NOT support Iceberg.
  - CDP Private Cloud CDW 1.5.1 Hive does support Iceberg.  You need CDW Data Services 1.5.1 or higher.
  - CDP Public Cloud Hive does support Iceberg as of August 2023 (Datahub and CDW)

### Caution

Make sure you know the component limitations with Iceberg Tables [here](https://docs.cloudera.com/cdp-public-cloud/cloud/cdp-iceberg/topics/iceberg-in-cdp.html) so you aren't caught by surprise.
