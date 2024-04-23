/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.cloudera.utils.hms.mirror.SessionVars.*;

/*
Provide a class where rules can be generated based on the hms-mirror stats collected.
 */
@Component
@Slf4j
@Getter
public class StatsCalculatorService {
    private HmsMirrorCfgService hmsMirrorCfgService;

    protected static Long getTezMaxGrouping(EnvironmentTable envTable) {
        SerdeType serdeType = serdeFromStats(envTable.getStatistics());

        Long maxGrouping = serdeType.getTargetSize() * 2L;

        if (envTable.getPartitioned()) {
            if (envTable.getStatistics().get(MirrorConf.AVG_FILE_SIZE) != null) {
                Double avgFileSize = (Double) envTable.getStatistics().get(MirrorConf.AVG_FILE_SIZE);
                // If not 90% of target size.
                if (avgFileSize < serdeType.getTargetSize() * .5) {
                    maxGrouping = (long) (serdeType.getTargetSize() / 2);
                }
            }
        }
        return maxGrouping;
    }
//    private static final Logger log = LoggerFactory.getLogger(StatsCalculator.class);

    private static SerdeType serdeFromStats(Map<String, Object> stats) {
        String sStype = stats.getOrDefault(MirrorConf.FILE_FORMAT, "UNKNOWN").toString();
        SerdeType serdeType = null;
        if (sStype == null) {
            serdeType = SerdeType.UNKNOWN;
        } else {
            try {
                serdeType = SerdeType.valueOf(sStype);
            } catch (IllegalArgumentException iae) {
                log.warn("Unable to determine type for file format: {}", sStype);
                serdeType = SerdeType.UNKNOWN;
            }
        }
        return serdeType;
    }

    public String getDistributedPartitionElements(EnvironmentTable envTable) {
        StringBuilder sb = new StringBuilder();
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        if (envTable.getPartitioned()) {

            if (hmsMirrorConfig.getOptimization().isAutoTune() &&
                    !hmsMirrorConfig.getOptimization().isSkipStatsCollection()) {
                SerdeType stype = serdeFromStats(envTable.getStatistics());
                if (envTable.getStatistics().get(MirrorConf.DATA_SIZE) != null) {
                    Long ratio = getPartitionDistributionRatio(envTable);
                    if (ratio >= 1) {
                        sb.append("ROUND((rand() * 1000) % ").append(ratio).append(")");
                    }
                }
            }

            // Place the partition element AFTER the sub grouping to ensure we get it applied in the plan.
            String partElement = TableUtils.getPartitionElements(envTable);
            if (partElement != null) {
                // Ensure we added element before placing comma.
                if (!sb.toString().isEmpty()) {
                    sb.append(", ");
                }
                sb.append(partElement);
            }
        }
        return sb.toString();
    }

    /*
    This will return the ratio of files to the average partition size. For example, if the average partition size is 1GB
    and the target size is 128MB, then the ratio will be 8. This means that we should have 8 files per partition.
        1GB / 128MB = 8
    (1024*1024*1024) / (128*1024*1024) = 8
     */
    protected Long getPartitionDistributionRatio(EnvironmentTable envTable) {
        Long ratio = 0L;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        if (!hmsMirrorConfig.getOptimization().isSkipStatsCollection()) {
            try {
                SerdeType stype = serdeFromStats(envTable.getStatistics());
                Long dataSize = (Long) envTable.getStatistics().get(MirrorConf.DATA_SIZE);
                Long avgPartSize = Math.floorDiv(dataSize, envTable.getPartitions().size());
                ratio = Math.floorDiv(avgPartSize, stype.getTargetSize()) - 1;
            } catch (RuntimeException rte) {
                log.warn("Unable to calculate partition distribution ratio for table: {}", envTable.getName());
            }
        }
        return ratio;
    }

    @Autowired
    public void setHmsMirrorCfgService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    public void setSessionOptions(Cluster cluster, EnvironmentTable controlEnv, EnvironmentTable applyEnv) {
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        // Skip if no stats collection.
        if (hmsMirrorConfig.getOptimization().isSkipStatsCollection())
            return;

        // Small File Checks
        SerdeType serdeType = serdeFromStats(controlEnv.getStatistics());
        // TODO: Trying to figure out if making this setting will bleed over to other sessions while reusing a connection.
        if (controlEnv.getStatistics().get(MirrorConf.AVG_FILE_SIZE) != null) {
            Double avgFileSize = (Double) controlEnv.getStatistics().get(MirrorConf.AVG_FILE_SIZE);
            // If not 50% of target size.
            if (avgFileSize < serdeType.getTargetSize() * .5) {
                applyEnv.addIssue("Setting " + TEZ_GROUP_MAX_SIZE + " to account for the sources 'small files'");
                // Set the tez group max size.
                Long maxGrouping = getTezMaxGrouping(controlEnv);
                applyEnv.addSql("Setting the " + TEZ_GROUP_MAX_SIZE,
                        "set " + TEZ_GROUP_MAX_SIZE + "=" + maxGrouping);
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
                                    (ratio * controlEnv.getPartitions().size()) + 20);
                } else {
                    applyEnv.addSql("Setting " + HIVE_MAX_REDUCERS,
                            "set " + HIVE_MAX_REDUCERS + "=" +
                                    (int) (controlEnv.getPartitions().size() * 1.2));
                }
            }
        }

        // Compression Settings.
        if (serdeType == SerdeType.TEXT) {
            if (hmsMirrorConfig.getOptimization().isCompressTextOutput()) {
                applyEnv.addIssue("Setting " + HIVE_COMPRESS_OUTPUT + " because you've setting that optimization");
                applyEnv.addSql("Setting: " + HIVE_COMPRESS_OUTPUT, "set " + HIVE_COMPRESS_OUTPUT + "=true");
            } else {
                applyEnv.addIssue("Setting " + HIVE_COMPRESS_OUTPUT + " because you HAVEN'T set that optimization");
                applyEnv.addSql("Setting: " + HIVE_COMPRESS_OUTPUT, "set " + HIVE_COMPRESS_OUTPUT + "=false");
            }
        }

        // Handle Auto Stats Gathering.
        if (!cluster.isLegacyHive()) {
            if (cluster.isEnableAutoTableStats()) {
                applyEnv.addIssue("Setting " + HIVE_AUTO_TABLE_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_TABLE_STATS, "set " + HIVE_AUTO_TABLE_STATS + "=true");
            } else {
                applyEnv.addIssue("Setting " + HIVE_AUTO_TABLE_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_TABLE_STATS, "set " + HIVE_AUTO_TABLE_STATS + "=false");
            }
            if (cluster.isEnableAutoColumnStats()) {
                applyEnv.addIssue("Setting " + HIVE_AUTO_COLUMN_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_COLUMN_STATS, "set " + HIVE_AUTO_COLUMN_STATS + "=true");
            } else {
                applyEnv.addIssue("Setting " + HIVE_AUTO_COLUMN_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_COLUMN_STATS, "set " + HIVE_AUTO_COLUMN_STATS + "=false");
            }
        }
    }
}
