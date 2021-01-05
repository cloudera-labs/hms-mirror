# Test Script
hms-mirror -h > $HOME/hms-mirror-test.out

# Dry run with default metadata process
hms-mirror -m -dr -db tpcds_bin_partitioned_orc_10
