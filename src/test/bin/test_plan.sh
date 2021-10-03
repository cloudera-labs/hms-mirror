#!/usr/bin/env bash

OUTPUT_DIR=temp/hms-mirror-reports
rm -rf ${OUTPUT_DIR}
rm $HOME/.hms-mirror/logs/hms-mirror.log

# Clean up DB's
hive -e 'DROP DATABASE IF EXISTS tpcds_bin_partitioned_orc_10 cascade'

for ds in SCHEMA_ONLY DUMP EXPORT_IMPORT SQL HYBRID; do
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -o ${OUTPUT_DIR}/${ds}
	hms-mirror -q -d ${ds} -ds RIGHT -db tpcds_bin_partitioned_orc_250 -o ${OUTPUT_DIR}/${ds}_right
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -o ${OUTPUT_DIR}/${ds}
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -v -o ${OUTPUT_DIR}/${ds}_v
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -mao -o ${OUTPUT_DIR}/${ds}_mao
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -ma -mnn -o ${OUTPUT_DIR}/${ds}_ma_mnn
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -mnn -o ${OUTPUT_DIR}/${ds}_mnn
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -mnn -o ${OUTPUT_DIR}/${ds}_is
  # Table Filter with RO
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -o ${OUTPUT_DIR}/${ds}_is -ro -e --accept -tf call_center.*
	# Sync
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -o ${OUTPUT_DIR}/${ds}_is -sync -ro -e --accept -tf call_center.*
	# All Sync
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -o ${OUTPUT_DIR}/${ds}_is -sync -ro -e --accept
	# Views
	hms-mirror -q -d ${ds} -db tpcds_bin_partitioned_orc_10 -o ${OUTPUT_DIR}/${ds}_is -sync -ro -v -e --accept
  # Drop DB for next run.
  hive -e 'DROP DATABASE IF EXISTS tpcds_bin_partitioned_orc_10 cascade'
done
