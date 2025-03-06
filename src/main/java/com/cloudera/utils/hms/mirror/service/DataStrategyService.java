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
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Getter
@Setter
public class DataStrategyService {

    private AcidDataStrategy acidDataStrategy = null;
    private CommonDataStrategy commonDataStrategy = null;
    private ConvertLinkedDataStrategy convertLinkedDataStrategy = null;
    private DumpDataStrategy dumpDataStrategy = null;
    private ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy = null;
    private ExportImportDataStrategy exportImportDataStrategy = null;
    private HybridDataStrategy hybridDataStrategy = null;
    private HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy = null;
    private LinkedDataStrategy linkedDataStrategy = null;
    private SchemaOnlyDataStrategy schemaOnlyDataStrategy = null;
    private StorageMigrationDataStrategy storageMigrationDataStrategy = null;
    private SQLDataStrategy sqlDataStrategy = null;
    private SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy = null;
    private IntermediateDataStrategy intermediateDataStrategy = null;
    private IcebergConversionDataStrategy icebergConversionDataStrategy = null;

    public DataStrategy getDefaultDataStrategy(HmsMirrorConfig hmsMirrorConfig) {
        DataStrategy dataStrategy = null;
        switch (hmsMirrorConfig.getDataStrategy()) {
            case STORAGE_MIGRATION:
                dataStrategy = storageMigrationDataStrategy;
                break;
            case ICEBERG_CONVERSION:
                dataStrategy = icebergConversionDataStrategy;
                break;
            case DUMP:
                dataStrategy = dumpDataStrategy;
                break;
            case EXPORT_IMPORT:
                dataStrategy = exportImportDataStrategy;
                break;
            case HYBRID:
                dataStrategy = hybridDataStrategy;
                break;
            case LINKED:
                dataStrategy = linkedDataStrategy;
                break;
            case SCHEMA_ONLY:
                dataStrategy = schemaOnlyDataStrategy;
                break;
            case SQL:
                dataStrategy = sqlDataStrategy;
                break;
            case CONVERT_LINKED:
                dataStrategy = convertLinkedDataStrategy;
                break;
            case COMMON:
                dataStrategy = commonDataStrategy;
                break;
            case INTERMEDIATE:
                dataStrategy = intermediateDataStrategy;
                break;
            case ACID:
                dataStrategy = acidDataStrategy;
                break;
            case HYBRID_ACID_DOWNGRADE_INPLACE:
                dataStrategy = hybridAcidDowngradeInPlaceDataStrategy;
                break;
            case SQL_ACID_DOWNGRADE_INPLACE:
                dataStrategy = sqlAcidInPlaceDataStrategy;
                break;
            case EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE:
                dataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
                break;
            default:
                dataStrategy = schemaOnlyDataStrategy;
        }
        return dataStrategy;
    }

    @Autowired
    public void setAcidDataStrategy(AcidDataStrategy acidDataStrategy) {
        this.acidDataStrategy = acidDataStrategy;
    }

    @Autowired
    public void setCommonDataStrategy(CommonDataStrategy commonDataStrategy) {
        this.commonDataStrategy = commonDataStrategy;
    }

    @Autowired
    public void setConvertLinkedDataStrategy(ConvertLinkedDataStrategy convertLinkedDataStrategy) {
        this.convertLinkedDataStrategy = convertLinkedDataStrategy;
    }

    @Autowired
    public void setDumpDataStrategy(DumpDataStrategy dumpDataStrategy) {
        this.dumpDataStrategy = dumpDataStrategy;
    }

    @Autowired
    public void setExportImportAcidDowngradeInPlaceDataStrategy(ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy) {
        this.exportImportAcidDowngradeInPlaceDataStrategy = exportImportAcidDowngradeInPlaceDataStrategy;
    }

    @Autowired
    public void setExportImportDataStrategy(ExportImportDataStrategy exportImportDataStrategy) {
        this.exportImportDataStrategy = exportImportDataStrategy;
    }

    @Autowired
    public void setHybridAcidDowngradeInPlaceDataStrategy(HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy) {
        this.hybridAcidDowngradeInPlaceDataStrategy = hybridAcidDowngradeInPlaceDataStrategy;
    }

    @Autowired
    public void setHybridDataStrategy(HybridDataStrategy hybridDataStrategy) {
        this.hybridDataStrategy = hybridDataStrategy;
    }

    @Autowired
    public void setIcebergConversionDataStrategy(IcebergConversionDataStrategy icebergConversionDataStrategy) {
        this.icebergConversionDataStrategy = icebergConversionDataStrategy;
    }

    @Autowired
    public void setIntermediateDataStrategy(IntermediateDataStrategy intermediateDataStrategy) {
        this.intermediateDataStrategy = intermediateDataStrategy;
    }

    @Autowired
    public void setLinkedDataStrategy(LinkedDataStrategy linkedDataStrategy) {
        this.linkedDataStrategy = linkedDataStrategy;
    }

    @Autowired
    public void setSchemaOnlyDataStrategy(SchemaOnlyDataStrategy schemaOnlyDataStrategy) {
        this.schemaOnlyDataStrategy = schemaOnlyDataStrategy;
    }

    @Autowired
    public void setSqlAcidInPlaceDataStrategy(SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy) {
        this.sqlAcidInPlaceDataStrategy = sqlAcidInPlaceDataStrategy;
    }

    @Autowired
    public void setSqlDataStrategy(SQLDataStrategy sqlDataStrategy) {
        this.sqlDataStrategy = sqlDataStrategy;
    }

    @Autowired
    public void setStorageMigrationDataStrategy(StorageMigrationDataStrategy storageMigrationDataStrategy) {
        this.storageMigrationDataStrategy = storageMigrationDataStrategy;
    }

}
