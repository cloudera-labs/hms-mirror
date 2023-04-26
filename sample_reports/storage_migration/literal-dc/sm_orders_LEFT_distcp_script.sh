#!/usr/bin/env sh

# 1. Copy the source '*_distcp_source.txt' files to the distributed filesystem.
# 2. Export an env var 'HCFS_BASE_DIR' that represents where these files where placed.
#      NOTE: ${HCFS_BASE_DIR} must be available to the user running 'distcp'
# 3. Export an env var 'DISTCP_OPTS' with any special settings needed to run the job.
#      For large jobs, you may need to adjust memory settings.
# 4. Run the following in an order or framework that is appropriate for your environment.
#       These aren't necessarily expected to run in this shell script as is in production.


if [ -z ${HCFS_BASE_DIR+x} ]; then
  echo "HCFS_BASE_DIR is unset"
  echo "What is the 'HCFS_BASE_DIR':"
  read HCFS_BASE_DIR
  echo "HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'"
else
  echo "HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'"
fi

echo "Creating HCFS directory: $HCFS_BASE_DIR"
hdfs dfs -mkdir -p $HCFS_BASE_DIR


echo "Copying 'distcp' source file to $HCFS_BASE_DIR"

hdfs dfs -copyFromLocal -f sm_orders_LEFT_1_distcp_source.txt ${HCFS_BASE_DIR}

echo "Running 'distcp'"
hadoop distcp ${DISTCP_OPTS} -f ${HCFS_BASE_DIR}/sm_orders_LEFT_1_distcp_source.txt ofs://OHOME90/user/dstreev/datasets


echo "Copying 'distcp' source file to $HCFS_BASE_DIR"

hdfs dfs -copyFromLocal -f sm_orders_LEFT_2_distcp_source.txt ${HCFS_BASE_DIR}

echo "Running 'distcp'"
hadoop distcp ${DISTCP_OPTS} -f ${HCFS_BASE_DIR}/sm_orders_LEFT_2_distcp_source.txt ofs://OHOME90/warehouse/tablespace/external/hive/sm_orders.db

