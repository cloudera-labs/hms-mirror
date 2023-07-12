package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.*;

/*
Provide a class where rules can be generated based on the hms-mirror stats collected.
 */
public class StatsCalculator {
    private static final Logger LOG = LogManager.getLogger(StatsCalculator.class);

    /*
    This will return the ratio of files to the average partition size. For example, if the average partition size is 1GB
    and the target size is 128MB, then the ratio will be 8. This means that we should have 8 files per partition.
        1GB / 128MB = 8
    (1024*1024*1024) / (128*1024*1024) = 8
     */
    protected static Long getPartitionDistributionRatio(EnvironmentTable envTable) {
        Long ratio = 0L;
        if (!Context.getInstance().getConfig().getOptimization().getSkipStatsCollection()) {
            try {
                SerdeType stype = (SerdeType) envTable.getStatistics().get(FILE_FORMAT);
                Long dataSize = (Long) envTable.getStatistics().get(DATA_SIZE);
                Long avgPartSize = Math.floorDiv(dataSize, (long) envTable.getPartitions().size());
                ratio = Math.floorDiv(avgPartSize, stype.targetSize) - 1;
            } catch (RuntimeException rte) {
                LOG.warn("Unable to calculate partition distribution ratio for table: " + envTable.getName());
            }
        }
        return ratio;
    }

    public static String getDistributedPartitionElements(EnvironmentTable envTable) {
        StringBuilder sb = new StringBuilder();

        if (envTable.getPartitioned()) {

            if (Context.getInstance().getConfig().getOptimization().getAutoTune() &&
                    !Context.getInstance().getConfig().getOptimization().getSkipStatsCollection()) {
                SerdeType stype = (SerdeType) envTable.getStatistics().get(FILE_FORMAT);
                if (stype != null) {
                    if (envTable.getStatistics().get(DATA_SIZE) != null) {
                        Long ratio = StatsCalculator.getPartitionDistributionRatio(envTable);
                        if (ratio >= 1) {
                            sb.append("ROUND((rand() * 1000) % ").append(ratio.toString()).append(")");
                        }
                    }
                }
            }

            // Place the partition element AFTER the sub grouping to ensure we get it applied in the plan.
            String partElement = TableUtils.getPartitionElements(envTable);
            if (partElement != null) {
                // Ensure we added element before placing comma.
                if (sb.toString().length() > 0) {
                    sb.append(", ");
                }
                sb.append(partElement);
            }
        }
        return sb.toString();
    }

    protected static Long getTezMaxGrouping(EnvironmentTable envTable) {
        SerdeType serdeType = (SerdeType) envTable.getStatistics().get(FILE_FORMAT);
        if (serdeType == null)
            serdeType = SerdeType.UNKNOWN;

        Long maxGrouping = serdeType.targetSize * 2L;

        if (envTable.getPartitioned()) {
            if (envTable.getStatistics().get(AVG_FILE_SIZE) != null) {
                Double avgFileSize = (Double) envTable.getStatistics().get(AVG_FILE_SIZE);
                // If not 90% of target size.
                if (avgFileSize < serdeType.targetSize * .5) {
                    maxGrouping = (long) (serdeType.targetSize / 2);
                }
            }
        }
        return maxGrouping;
    }

    public static void setSessionOptions(Cluster cluster, EnvironmentTable controlEnv, EnvironmentTable applyEnv) {

        // Skip if no stats collection.
        if (Context.getInstance().getConfig().getOptimization().getSkipStatsCollection())
            return;

        // Small File settings.
        SerdeType stype = (SerdeType) controlEnv.getStatistics().get(FILE_FORMAT);
        if (stype != null) {
            // TODO: Trying to figure out if making this setting will bleed over to other sessions while reusing a connection.
            if (controlEnv.getStatistics().get(AVG_FILE_SIZE) != null) {
                Double avgFileSize = (Double) controlEnv.getStatistics().get(AVG_FILE_SIZE);
                // If not 50% of target size.
                if (avgFileSize < stype.targetSize * .5) {
                    applyEnv.addIssue("Setting " + TEZ_GROUP_MAX_SIZE + " to account for the sources 'small files'");
                    // Set the tez group max size.
                    Long maxGrouping = getTezMaxGrouping(controlEnv);
                    applyEnv.addSql("Setting the " + TEZ_GROUP_MAX_SIZE,
                            "set " + TEZ_GROUP_MAX_SIZE + "=" + maxGrouping);
                }
            }
        }

        // Check the partition count.
        if (controlEnv.getPartitioned()) {
            // MAX DYN PARTS: 1000 is the Apache default.  CDP is 5000.  Regardless, we'll set this to +20%
            // Also check MAX REDUCERS
            if (controlEnv.getPartitions().size() > 1000) {
                applyEnv.addIssue("Setting " + HIVE_MAX_DYNAMIC_PARTITIONS);
                applyEnv.addSql("Setting " + HIVE_MAX_DYNAMIC_PARTITIONS,
                        "set " + HIVE_MAX_DYNAMIC_PARTITIONS + "=" +
                                (int) (controlEnv.getPartitions().size() * 1.2));
                applyEnv.addIssue("Adjusting " + HIVE_MAX_REDUCERS + " to handle partition load");
                int ratio = getPartitionDistributionRatio(controlEnv).intValue();
                if (ratio >= 1) {
                    applyEnv.addSql("Setting " + HIVE_MAX_REDUCERS,
                            "set " + HIVE_MAX_REDUCERS + "=" +
                                    (int) (ratio * controlEnv.getPartitions().size()) + 20);
                } else {
                    applyEnv.addSql("Setting " + HIVE_MAX_REDUCERS,
                            "set " + HIVE_MAX_REDUCERS + "=" +
                                    (int) (controlEnv.getPartitions().size() * 1.2));
                }
            }
        }

        // Compression Settings.
        if (controlEnv.getStatistics().get(FILE_FORMAT) != null
                && controlEnv.getStatistics().get(FILE_FORMAT) == SerdeType.TEXT) {
            if (Context.getInstance().getConfig().getOptimization().getCompressTextOutput()) {
                applyEnv.addIssue("Setting " + HIVE_COMPRESS_OUTPUT + " because you've setting that optimization");
                applyEnv.addSql("Setting: " + HIVE_COMPRESS_OUTPUT, "set " + HIVE_COMPRESS_OUTPUT + "=true");
            } else {
                applyEnv.addIssue("Setting " + HIVE_COMPRESS_OUTPUT + " because you HAVEN'T set that optimization");
                applyEnv.addSql("Setting: " + HIVE_COMPRESS_OUTPUT, "set " + HIVE_COMPRESS_OUTPUT + "=false");
            }
        }

        // Handle Auto Stats Gathering.
        if (cluster.getEnableAutoTableStats()) {
            applyEnv.addIssue("Setting " + HIVE_AUTO_TABLE_STATS + " because you've set that optimization");
            applyEnv.addSql("Setting: " + HIVE_AUTO_TABLE_STATS, "set " + HIVE_AUTO_TABLE_STATS + "=true");
        } else {
            applyEnv.addIssue("Setting " + HIVE_AUTO_TABLE_STATS + " because you've set that optimization");
            applyEnv.addSql("Setting: " + HIVE_AUTO_TABLE_STATS, "set " + HIVE_AUTO_TABLE_STATS + "=false");
        }
        if (cluster.getEnableAutoColumnStats()) {
            applyEnv.addIssue("Setting " + HIVE_AUTO_COLUMN_STATS + " because you've set that optimization");
            applyEnv.addSql("Setting: " + HIVE_AUTO_COLUMN_STATS, "set " + HIVE_AUTO_COLUMN_STATS + "=true");
        } else {
            applyEnv.addIssue("Setting " + HIVE_AUTO_COLUMN_STATS + " because you've set that optimization");
            applyEnv.addSql("Setting: " + HIVE_AUTO_COLUMN_STATS, "set " + HIVE_AUTO_COLUMN_STATS + "=false");
        }
    }
}
