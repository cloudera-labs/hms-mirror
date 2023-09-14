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

import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum;

public class StorageMigration {

    private DataStrategyEnum strategy = DataStrategyEnum.SQL;
    private Boolean distcp = Boolean.FALSE;
    private DistcpFlow dataFlow = DistcpFlow.PULL;

    public DataStrategyEnum getStrategy() {
        return strategy;
    }

    public void setStrategy(DataStrategyEnum strategy) {
        switch (strategy) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
                this.strategy = strategy;
                break;
            default:
                throw new RuntimeException("Invalid strategy for STORAGE_MIGRATION");
        }
    }

    public DistcpFlow getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(DistcpFlow dataFlow) {
        this.dataFlow = dataFlow;
    }

    public Boolean isDistcp() {
        return distcp;
    }

    public void setDistcp(Boolean distcp) {
        this.distcp = distcp;
    }
}
