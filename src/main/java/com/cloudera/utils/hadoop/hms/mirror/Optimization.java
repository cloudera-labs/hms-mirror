/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.Context;

public class Optimization {

    /*
    Control whether we'll set the 'hive.optimize.sort.dynamic.partition` conf to 'true' or not.  If this is not set,
    we'll use a PRESCRIPTIVE approach with the transfer SQL on partitioned tables by adding a DISTRIBUTE BY clause.
     */
    private Boolean sortDynamicPartitionInserts = Boolean.FALSE;
    /*
    Skip all optimizations by setting:
    - hive.optimize.sort.dynamic.partition=false
    - Not using DISTRIBUTE BY.
    - But do include additional settings specified by user in 'overrides'.
     */
    private Boolean skip = Boolean.FALSE;
    private Boolean autoTune = Boolean.FALSE;
    private Boolean compressTextOutput = Boolean.FALSE;
    private Boolean skipStatsCollection = Boolean.FALSE;

    private Overrides overrides = new Overrides();
    private Boolean buildShadowStatistics = Boolean.FALSE;

    public Boolean getAutoTune() {
        return autoTune;
    }

    public void setAutoTune(Boolean autoTune) {
        this.autoTune = autoTune;
    }

    public Boolean getBuildShadowStatistics() {
        return buildShadowStatistics;
    }

    public void setBuildShadowStatistics(Boolean buildShadowStatistics) {
        this.buildShadowStatistics = buildShadowStatistics;
    }

    public Boolean getCompressTextOutput() {
        return compressTextOutput;
    }

    public void setCompressTextOutput(Boolean compressTextOutput) {
        this.compressTextOutput = compressTextOutput;
    }

    public Overrides getOverrides() {
        return overrides;
    }

    public void setOverrides(Overrides overrides) {
        this.overrides = overrides;
    }

    public Boolean getSkip() {
        return skip;
    }

    public void setSkip(Boolean skip) {
        this.skip = skip;
    }

    public Boolean getSkipStatsCollection() {
        // Reset skipStatsCollection to true if we're doing a dump or schema only. (and a few other conditions)
        if (skipStatsCollection != null && !skipStatsCollection) {
            try {
                switch (Context.getInstance().getConfig().getDataStrategy()) {
                    case DUMP:
                    case SCHEMA_ONLY:
                    case EXPORT_IMPORT:
                        skipStatsCollection = Boolean.TRUE;
                        break;
                    case STORAGE_MIGRATION:
                        if (Context.getInstance().getConfig().getTransfer().getStorageMigration().isDistcp()) {
                            skipStatsCollection = Boolean.TRUE;
                        }
                        break;
                }
            } catch (NullPointerException npe) {
                // Ignore: Caused during 'setup' since the context and config don't exist.
            }
        }
        return skipStatsCollection;
    }

    public void setSkipStatsCollection(Boolean skipStatsCollection) {
        this.skipStatsCollection = skipStatsCollection;
    }

    public Boolean getSortDynamicPartitionInserts() {
        return sortDynamicPartitionInserts;
    }

    public void setSortDynamicPartitionInserts(Boolean sortDynamicPartitionInserts) {
        this.sortDynamicPartitionInserts = sortDynamicPartitionInserts;
    }

}
