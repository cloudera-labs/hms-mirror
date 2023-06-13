package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.util.TableUtils;

import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hadoop.hms.mirror.SessionVars.*;

/*
Provide a class where rules can be generated based on the hms-mirror stats collected.
 */
public class StatsCalculator {

    public static String getAdditionalPartitionDistribution(EnvironmentTable envTable) {
        StringBuilder sb = new StringBuilder();

        if (envTable.getPartitioned()) {
            SerdeType stype = (SerdeType)envTable.getStatistics().get(FILE_FORMAT);
            if (stype != null) {
                if (envTable.getStatistics().get(DATA_SIZE) != null) {
                    Long dataSize = (Long)envTable.getStatistics().get(DATA_SIZE);
                    Long avgPartSize = Math.floorDiv(dataSize, (long)envTable.getPartitions().size());
                    Long ratio = Math.floorDiv(avgPartSize, stype.targetSize) - 1;
                    if (ratio >= 1) {
                        sb.append("ROUND((rand() * 1000) % ").append(ratio.toString()).append(")");
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

    public static String getTezMaxGrouping(EnvironmentTable envTable) {
        StringBuilder sb = new StringBuilder(TableUtils.getPartitionElements(envTable));

        if (envTable.getPartitioned()) {
            String stype = (String)envTable.getStatistics().get(FILE_FORMAT);
            if (stype != null) {
                SerdeType serdeType = SerdeType.valueOf(stype);
                if (envTable.getStatistics().get(AVG_FILE_SIZE) != null) {
                    Double avgFileSize = (Double)envTable.getStatistics().get(AVG_FILE_SIZE);
                    // If not 90% of target size.
                    if (avgFileSize < serdeType.targetSize * .9) {

                    }
                }
            }
        }

        return sb.toString();
    }

    public static void setSessionOptions(EnvironmentTable controlEnv, EnvironmentTable applyEnv) {
        // Small File settings.
        SerdeType stype = (SerdeType) controlEnv.getStatistics().get(FILE_FORMAT);
        if (stype != null) {
            // TODO: Trying to figure out if making this setting will bleed over to other sessions while reusing a connection.
            if (controlEnv.getStatistics().get(AVG_FILE_SIZE) != null) {
                Double avgFileSize = (Double)controlEnv.getStatistics().get(AVG_FILE_SIZE);
                // If not 50% of target size.
                if (avgFileSize < stype.targetSize * .5) {
                    applyEnv.addIssue("Setting " + TEZ_GROUP_MAX_SIZE + " to account for the sources 'small files'");
                    // Set the tez group max size.
                    applyEnv.addSql("Setting the " + TEZ_GROUP_MAX_SIZE,
                            "set " +TEZ_GROUP_MAX_SIZE + "=" + stype.targetSize);
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
                applyEnv.addSql("Setting " + HIVE_MAX_REDUCERS,
                        "set " + HIVE_MAX_REDUCERS + "=" +
                                Double.toString(controlEnv.getPartitions().size() * 2));
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
    }
}
