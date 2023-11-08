# On Prem Legacy Hive to Non-Legacy Hive

## Environments

HDP 2.6 -> CDP
CDH 5.x -> CDP
CDH 6.x -> CDP

LEFT clusters are the LEGACY clusters.
RIGHT clusters are the NON-LEGACY clusters (hive3).

## Assumptions

- LEFT HS2 is NOT Kerberized.  Since this is a Legacy cluster (which is kerberized) we need to establish a 'non' kerberized HS2 endpoint.  Use KNOX or setup an additional HS2 in the management console that isn't Kerberized.  hms-mirror is built (default) with Hadoop 3, of which the libraries are NOT backwardly compatible.
- Standalone JDBC jar files for the LEGACY and NON-LEGACY clusters are available to the running host as specified in the 'configuration'.
- `hms-mirror` is run from an EdgeNode on CDP
  - The edgenode has network access to the Legacy HS2 endpoint
- No ACID tables (HDP)
- No VIEWs
- No Non-Native tables (Hive tables backed by HBase, JDBC, Kafka)
- The HiveServer2's on each cluster have enough concurrency to support the configured connections `transfer->concurrency`.  If not specified, the default is 4.

## NOTES

`hms-mirror` runs in DRY-RUN mode by default.  Add `-e|--execute` to your command to actually run the process on the clusters.  Use `--accept` to avoid the verification questions (but don't deny their meaning).

All actions performed by `hms-mirror` are recorded in the *_execute.sql files.  Review them to understand the orchestration and process.

Review the report markdown files (html version also available) for details about the job.  Explanations regarding steps, issues, and failure reasons can be found there.

