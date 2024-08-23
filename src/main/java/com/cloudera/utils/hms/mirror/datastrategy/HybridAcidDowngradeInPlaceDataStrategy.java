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

package com.cloudera.utils.hms.mirror.datastrategy;

import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Getter
public class HybridAcidDowngradeInPlaceDataStrategy extends DataStrategyBase implements DataStrategy {

    private SQLAcidDowngradeInPlaceDataStrategy sqlAcidDowngradeInPlaceDataStrategy;

    private ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;

    public HybridAcidDowngradeInPlaceDataStrategy(ExecuteSessionService executeSessionService, TranslatorService translatorService) {
        this.executeSessionService = executeSessionService;
        this.translatorService = translatorService;
    }

    @Override
    public Boolean buildOutDefinition(TableMirror tableMirror) {
        return null;
    }

    @Override
    public Boolean buildOutSql(TableMirror tableMirror) throws MissingDataPointException {
        return null;
    }

    @Override
    public Boolean execute(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();
        /*
        Check environment is Hive 3.
            if not, need to do SQLACIDInplaceDowngrade.
        If table is not partitioned
            go to export import downgrade inplace
        else if partitions <= hybrid.exportImportPartitionLimit
            go to export import downgrade inplace
        else if partitions <= hybrid.sqlPartitionLimit
            go to sql downgrade inplace
        else
            too many partitions.
         */
        if (hmsMirrorConfig.getCluster(Environment.LEFT).isLegacyHive()) {
            rtn = sqlAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
        } else {
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < hmsMirrorConfig.getHybrid().getExportImportPartitionLimit() ||
                        hmsMirrorConfig.getHybrid().getExportImportPartitionLimit() <= 0) {
                    rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
                } else {
                    rtn = sqlAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
                }
            } else {
                // Go with EXPORT_IMPORT
                rtn = exportImportAcidDowngradeInPlaceDataStrategy.execute(tableMirror);
            }
        }
        return rtn;
    }

    @Autowired
    public void setExportImportAcidDowngradeInPlaceDataStrategy(ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy) {
        this.exportImportAcidDowngradeInPlaceDataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
    }

    @Autowired
    public void setSqlAcidDowngradeInPlaceDataStrategy(SQLAcidDowngradeInPlaceDataStrategy sqlAcidDowngradeInPlaceDataStrategy) {
        this.sqlAcidDowngradeInPlaceDataStrategy = sqlAcidDowngradeInPlaceDataStrategy;
    }
}
