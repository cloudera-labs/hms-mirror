`hms-mirror -db tpcds_bin_partitioned_orc_10 -o /home/dstreev/temp/hms-mirror-reports -sql`

or

`hms-mirror -d SCHEMA_ONLY -db tpcds_bin_partitioned_orc_10 -o /home/dstreev/temp/hms-mirror-reports -sql`


NOTES
- SCHEMA_ONLY is the default `-d` option.  When `-d` isn't specified, SCHEMA_ONLY is run.
- When `-e` is NOT present, `hms-mirror` runs in a DRY-RUN mode.  No actions are performed.

[The Executed/able Script to reproduce the schema](./tpcds_bin_partitioned_orc_10_execute.sql)

[The job report](./tpcds_bin_partitioned_orc_10_hms-mirror.md) (html version available)

[LEFT Action Report](./tpcds_bin_partitioned_orc_10_LEFT_action.sql) - used as followup actions on LEFT cluster

[RIGHT Action Report](./tpcds_bin_partitioned_orc_10_RIGHT_action.sql) - used as followup actions on RIGHT cluster

[The distcp workbook for schemas reviewed](./distcp_workbook.md)