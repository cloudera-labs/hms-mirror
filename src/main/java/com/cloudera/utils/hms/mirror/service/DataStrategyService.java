/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.datastrategy.*;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;

/**
 * Service class responsible for managing and providing the appropriate data strategy
 * based on a given configuration. This class contains multiple types of data strategies
 * and determines which one to use depending on the data strategy specified in the provided configuration.
 *
 * Each data strategy is injected into the service using Spring's dependency injection mechanism.
 */
@Service
public class DataStrategyService {
    private final Map<DataStrategyEnum, DataStrategy> strategies;
    private final DataStrategy defaultStrategy;

    public DataStrategyService(
            AcidDataStrategy acidDataStrategy,
            CommonDataStrategy commonDataStrategy,
            ConvertLinkedDataStrategy convertLinkedDataStrategy,
            DumpDataStrategy dumpDataStrategy,
            ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy,
            ExportImportDataStrategy exportImportDataStrategy,
            HybridDataStrategy hybridDataStrategy,
            HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy,
            LinkedDataStrategy linkedDataStrategy,
            SchemaOnlyDataStrategy schemaOnlyDataStrategy,
            StorageMigrationDataStrategy storageMigrationDataStrategy,
            SQLDataStrategy sqlDataStrategy,
            SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy,
            IntermediateDataStrategy intermediateDataStrategy,
            IcebergConversionDataStrategy icebergConversionDataStrategy) {
        
        this.defaultStrategy = schemaOnlyDataStrategy;
        this.strategies = new EnumMap<>(DataStrategyEnum.class);
        
        strategies.put(DataStrategyEnum.STORAGE_MIGRATION, storageMigrationDataStrategy);
        strategies.put(DataStrategyEnum.ICEBERG_CONVERSION, icebergConversionDataStrategy);
        strategies.put(DataStrategyEnum.DUMP, dumpDataStrategy);
        strategies.put(DataStrategyEnum.EXPORT_IMPORT, exportImportDataStrategy);
        strategies.put(DataStrategyEnum.HYBRID, hybridDataStrategy);
        strategies.put(DataStrategyEnum.LINKED, linkedDataStrategy);
        strategies.put(DataStrategyEnum.SCHEMA_ONLY, schemaOnlyDataStrategy);
        strategies.put(DataStrategyEnum.SQL, sqlDataStrategy);
        strategies.put(DataStrategyEnum.CONVERT_LINKED, convertLinkedDataStrategy);
        strategies.put(DataStrategyEnum.COMMON, commonDataStrategy);
        strategies.put(DataStrategyEnum.INTERMEDIATE, intermediateDataStrategy);
        strategies.put(DataStrategyEnum.ACID, acidDataStrategy);
        strategies.put(DataStrategyEnum.HYBRID_ACID_DOWNGRADE_INPLACE, hybridAcidDowngradeInPlaceDataStrategy);
        strategies.put(DataStrategyEnum.SQL_ACID_DOWNGRADE_INPLACE, sqlAcidInPlaceDataStrategy);
        strategies.put(DataStrategyEnum.EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE, exportImportAcidDowngradeInPlaceDataStrategy);
    }

    public DataStrategy getDefaultDataStrategy(HmsMirrorConfig config) {
        return strategies.getOrDefault(config.getDataStrategy(), defaultStrategy);
    }
}