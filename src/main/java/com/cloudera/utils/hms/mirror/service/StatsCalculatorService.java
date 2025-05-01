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

import com.cloudera.utils.hms.mirror.MirrorConf;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.SerdeType;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Map;

import static com.cloudera.utils.hms.mirror.SessionVars.*;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The StatsCalculatorService class is responsible for performing various calculations and settings
 * related to the optimization and handling of statistics within the context of Hive-based tables
 * and environments (HMS Mirror). This service works with collected statistics to help configure
 * and optimize operations in big data tasks by using configuration options and rules derived from
 * current table and partition statistics.
 *
 * Key functionalities provided by this service include:
 * - Calculating max grouping size for Tez jobs based on target size and partition statistics.
 * - Generating partition distribution elements dynamically for optimization purposes.
 * - Determining distribution ratios of files to average partition size for more efficient processing.
 * - Configuring and adjusting session options for different cluster environments and tables, such as:
 *   - Adjusting settings for partition counts, reducers, and Tez grouping.
 *   - Managing compression capabilities for various file formats.
 *   - Enabling or disabling auto stats gathering at the table and column level.
 *
 * Dependencies:
 * - ExecuteSessionService: Used to retrieve runtime session configuration and execute tasks.
 *
 * The class also utilizes helper methods and configurations to infer SerdeType from given table
 * statistics and handle runtime warnings during runtime calculations or configurations to
 * ensure safe and reliable operations.
 */
@Component
@Slf4j
@Getter
public class StatsCalculatorService {
    private final ExecuteSessionService executeSessionService;

    public StatsCalculatorService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    protected static Long getTezMaxGrouping(EnvironmentTable envTable) {
        SerdeType serdeType = serdeFromStats(envTable.getStatistics());
        Long maxGrouping = serdeType.getTargetSize() * 2L;
        if (envTable.getPartitioned()) {
            if (nonNull(envTable.getStatistics().get(MirrorConf.AVG_FILE_SIZE))) {
                Double avgFileSize = (Double) envTable.getStatistics().get(MirrorConf.AVG_FILE_SIZE);
                // If not 90% of target size.
                if (avgFileSize < serdeType.getTargetSize() * .5) {
                    maxGrouping = (long) (serdeType.getTargetSize() / 2);
                }
            }
        }
        return maxGrouping;
    }

    private static SerdeType serdeFromStats(Map<String, Object> stats) {
        String sStype = stats.getOrDefault(MirrorConf.FILE_FORMAT, "UNKNOWN").toString();
        SerdeType serdeType = null;
        if (isNull(sStype)) {
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

    /**
     * Constructs and returns a string representing the distributed partition elements
     * for the given environment table. This includes distribution logic based on
     * statistics and partition configurations if applicable.
     *
     * @param envTable the environment table representing the source data, including
     *                 its partitioning information and associated statistics.
     * @return a string that outlines the distributed partition elements to be used
     *         in processing; if no partitioning or applicable distribution logic
     *         exists, an empty string is returned.
     */
    public String getDistributedPartitionElements(EnvironmentTable envTable) {
        StringBuilder sb = new StringBuilder();
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        if (envTable.getPartitioned()) {
            if (hmsMirrorConfig.getOptimization().isAutoTune() &&
                    !hmsMirrorConfig.getOptimization().isSkipStatsCollection()) {
                SerdeType stype = serdeFromStats(envTable.getStatistics());
                if (nonNull(envTable.getStatistics().get(MirrorConf.DATA_SIZE))) {
                    Long ratio = getPartitionDistributionRatio(envTable);
                    if (ratio >= 1) {
                        sb.append("ROUND((rand() * 1000) % ").append(ratio).append(")");
                    }
                }
            }
            // Place the partition element AFTER the sub grouping to ensure we get it applied in the plan.
            String partElement = TableUtils.getPartitionElements(envTable);
            if (!isBlank(partElement)) {
                // Ensure we added element before placing comma.
                if (!sb.toString().isEmpty()) {
                    sb.append(", ");
                }
                sb.append(partElement);
            }
        }
        return sb.toString();
    }

    /**
     * Calculates the ratio of files to the average partition size for the provided environment table.
     * The ratio helps determine the distribution of files per partition based on data size and
     * partition statistics.
     *
     * For example, if the average partition size is 1GB
     * and the target size is 128MB, then the ratio will be 8. This means that we should have 8 files per partition.
     *         1GB / 128MB = 8
     *     (1024*1024*1024) / (128*1024*1024) = 8
     *
     * @param envTable the environment table containing partition information and associated
     *                 statistics, which are used to calculate the distribution ratio.
     * @return the computed partition distribution ratio as a Long. If ratio calculation fails
     *         due to missing data or runtime exceptions, the method returns 0.
     */
    protected Long getPartitionDistributionRatio(EnvironmentTable envTable) {
        Long ratio = 0L;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        if (!hmsMirrorConfig.getOptimization().isSkipStatsCollection()) {
            try {
                SerdeType stype = serdeFromStats(envTable.getStatistics());
                Long dataSize = (Long) envTable.getStatistics().get(MirrorConf.DATA_SIZE);
                Long avgPartSize = Math.floorDiv(dataSize, Long.valueOf(envTable.getPartitions().size()));
                ratio = Math.floorDiv(avgPartSize, Long.valueOf(stype.getTargetSize())) - 1;
            } catch (RuntimeException rte) {
                log.warn("Unable to calculate partition distribution ratio for table: {}", envTable.getName());
            }
        }
        return ratio;
    }

    /**
     * Configures session options for the specified cluster and environment tables.
     * This method adjusts session-level configurations such as statistics collection,
     * small file handling, partition count, compression, and auto-stats gathering
     * based on the provided environment and cluster settings.
     *
     * @param cluster the cluster instance representing the current execution context,
     *                including settings that influence statistics and optimization behavior.
     * @param controlEnv the environment table that provides control or source-side statistics
     *                   and configuration data, which is analyzed for session option adjustments.
     * @param applyEnv the environment table where session configurations and adjustments
     *                 are applied, including any logged issues or SQL settings.
     */
    public void setSessionOptions(Cluster cluster, EnvironmentTable controlEnv, EnvironmentTable applyEnv) {
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        // Skip if no stats collection.
        if (hmsMirrorConfig.getOptimization().isSkipStatsCollection())
            return;
        // Small File Checks
        SerdeType serdeType = serdeFromStats(controlEnv.getStatistics());
        // TODO: Trying to figure out if making this setting will bleed over to other sessions while reusing a connections.
        if (nonNull(controlEnv.getStatistics().get(MirrorConf.AVG_FILE_SIZE))) {
            Double avgFileSize = (Double) controlEnv.getStatistics().get(MirrorConf.AVG_FILE_SIZE);
            // If not 50% of target size.
            if (avgFileSize < serdeType.getTargetSize() * .5) {
                applyEnv.addIssue("Setting " + TEZ_GROUP_MAX_SIZE + " to account for the sources 'small files'");
                // Set the tez group max size.
                Long maxGrouping = getTezMaxGrouping(controlEnv);
                applyEnv.addSql("Setting the " + TEZ_GROUP_MAX_SIZE,
                        MessageFormat.format(SET_SESSION_VALUE_INT, TEZ_GROUP_MAX_SIZE, maxGrouping));
            }
        }
        // Check the partition count.
        if (controlEnv.getPartitioned()) {
            // MAX DYN PARTS: 1000 is the Apache default.  CDP is 5000.  Regardless, we'll set this to +20%
            // Also check MAX REDUCERS
            if (controlEnv.getPartitions().size() > 1000) {
                applyEnv.addIssue("Setting " + HIVE_MAX_DYNAMIC_PARTITIONS);
                applyEnv.addSql("Setting " + HIVE_MAX_DYNAMIC_PARTITIONS,
                        MessageFormat.format(SET_SESSION_VALUE_INT, HIVE_MAX_DYNAMIC_PARTITIONS,
                                (int) (controlEnv.getPartitions().size() * 1.2)));
                applyEnv.addIssue("Adjusting " + HIVE_MAX_REDUCERS + " to handle partition load");
                int ratio = getPartitionDistributionRatio(controlEnv).intValue();
                if (ratio >= 1) {
                    applyEnv.addSql("Setting " + HIVE_MAX_REDUCERS,
                            MessageFormat.format(SET_SESSION_VALUE_INT, HIVE_MAX_REDUCERS,
                                    (ratio * controlEnv.getPartitions().size()) + 20));
                } else {
                    applyEnv.addSql("Setting " + HIVE_MAX_REDUCERS,
                            MessageFormat.format(SET_SESSION_VALUE_INT, HIVE_MAX_REDUCERS,
                                    (int) (controlEnv.getPartitions().size() * 1.2)));
                }
            }
        }
        // Compression Settings.
        if (serdeType == SerdeType.TEXT) {
            if (hmsMirrorConfig.getOptimization().isCompressTextOutput()) {
                applyEnv.addIssue("Setting " + HIVE_COMPRESS_OUTPUT + " because you've setting that optimization");
                applyEnv.addSql("Setting: " + HIVE_COMPRESS_OUTPUT, MessageFormat.format(SET_SESSION_VALUE_STRING, HIVE_COMPRESS_OUTPUT, "true"));
            } else {
                applyEnv.addIssue("Setting " + HIVE_COMPRESS_OUTPUT + " because you HAVEN'T set that optimization");
                applyEnv.addSql("Setting: " + HIVE_COMPRESS_OUTPUT, MessageFormat.format(SET_SESSION_VALUE_STRING, HIVE_COMPRESS_OUTPUT, "false"));
            }
        }

        // Handle Auto Stats Gathering.
        if (!cluster.isLegacyHive()) {
            if (cluster.isEnableAutoTableStats()) {
                applyEnv.addIssue("Setting " + HIVE_AUTO_TABLE_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_TABLE_STATS, MessageFormat.format(SET_SESSION_VALUE_STRING, HIVE_AUTO_TABLE_STATS, "true"));
            } else {
                applyEnv.addIssue("Setting " + HIVE_AUTO_TABLE_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_TABLE_STATS, MessageFormat.format(SET_SESSION_VALUE_STRING, HIVE_AUTO_TABLE_STATS, "false"));
            }
            if (cluster.isEnableAutoColumnStats()) {
                applyEnv.addIssue("Setting " + HIVE_AUTO_COLUMN_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_COLUMN_STATS, MessageFormat.format(SET_SESSION_VALUE_STRING, HIVE_AUTO_COLUMN_STATS, "true"));
            } else {
                applyEnv.addIssue("Setting " + HIVE_AUTO_COLUMN_STATS + " because you've set that optimization");
                applyEnv.addSql("Setting: " + HIVE_AUTO_COLUMN_STATS, MessageFormat.format(SET_SESSION_VALUE_STRING, HIVE_AUTO_COLUMN_STATS, "false"));
            }
        }
    }
}