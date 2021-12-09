#!/usr/bin/env bash
# Copyright 2021 Cloudera, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
