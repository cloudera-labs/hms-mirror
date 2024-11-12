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

package com.cloudera.utils.hms.mirror.domain;

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.utils.ConfigTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Objects.isNull;
import static org.junit.Assert.*;

@Slf4j
public class HmsMirrorConfigCloneFullTest_01 {

    private HmsMirrorConfig config;

    @Before
    public void setUp() throws IOException {
        log.info("Setting up HmsMirrorConfigCloneTest");
        config = ConfigTest.deserializeResource("/config/clone/full_test_01.yaml");
    }

    @Test
    public void cloneTest_01() {
        log.info("Test 01");
        HmsMirrorConfig clone = config.clone();

        // Acceptance Criteria:
        assertEquals(clone.getAcceptance().isSilentOverride(), config.getAcceptance().isSilentOverride());
        assertEquals(clone.getAcceptance().isBackedUpHDFS(), config.getAcceptance().isBackedUpHDFS());
        assertEquals(clone.getAcceptance().isBackedUpMetastore(), config.getAcceptance().isBackedUpMetastore());
        assertEquals(clone.getAcceptance().isTrashConfigured(), config.getAcceptance().isTrashConfigured());
        assertEquals(clone.getAcceptance().isPotentialDataLoss(), config.getAcceptance().isPotentialDataLoss());

        // Clusters
        List<Environment> environments = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        for (Environment environment : environments) {
            //
            assertEquals(clone.getCluster(environment).getHcfsNamespace(), config.getCluster(environment).getHcfsNamespace());
            assertEquals(clone.getCluster(environment).isLegacyHive(), config.getCluster(environment).isLegacyHive());
            assertEquals(clone.getCluster(environment).isCreateIfNotExists(), config.getCluster(environment).isCreateIfNotExists());
            assertEquals(clone.getCluster(environment).isHdpHive3(), config.getCluster(environment).isHdpHive3());
            assertEquals(clone.getCluster(environment).isCreateIfNotExists(), config.getCluster(environment).isCreateIfNotExists());
            assertEquals(clone.getCluster(environment).isEnableAutoColumnStats(), config.getCluster(environment).isEnableAutoColumnStats());
            assertEquals(clone.getCluster(environment).isEnableAutoTableStats(), config.getCluster(environment).isEnableAutoTableStats());
            // Hive Server 2
            if (isNull(config.getCluster(environment).getHiveServer2())) {
                assertNull(clone.getCluster(environment).getHiveServer2());
            } else {
                assertEquals(clone.getCluster(environment).getHiveServer2().getUri(), config.getCluster(environment).getHiveServer2().getUri());
                assertEquals(clone.getCluster(environment).getHiveServer2().isDisconnected(), config.getCluster(environment).getHiveServer2().isDisconnected());
                assertEquals(clone.getCluster(environment).getHiveServer2().getDriverClassName(), config.getCluster(environment).getHiveServer2().getDriverClassName());
                assertEquals(clone.getCluster(environment).getHiveServer2().getJarFile(), config.getCluster(environment).getHiveServer2().getJarFile());
                // Connection Properties
                assertEquals(clone.getCluster(environment).getHiveServer2().getConnectionProperties().get("user"), config.getCluster(environment).getHiveServer2().getConnectionProperties().get("user"));
                assertEquals(clone.getCluster(environment).getHiveServer2().getConnectionProperties().get("password"), config.getCluster(environment).getHiveServer2().getConnectionProperties().get("password"));

            }

            // Metastore
            if (isNull(config.getCluster(environment).getMetastoreDirect())) {
                assertNull(clone.getCluster(environment).getMetastoreDirect());
            } else {
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getUri(), config.getCluster(environment).getMetastoreDirect().getUri());
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getType(), config.getCluster(environment).getMetastoreDirect().getType());
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getInitSql(), config.getCluster(environment).getMetastoreDirect().getInitSql());
                // Connection Properties
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("user"), config.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("user"));
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("password"), config.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("password"));
            }

            assertEquals(clone.getCluster(environment).getPartitionDiscovery().isAuto(), config.getCluster(environment).getPartitionDiscovery().isAuto());
            assertEquals(clone.getCluster(environment).getPartitionDiscovery().isInitMSCK(), config.getCluster(environment).getPartitionDiscovery().isInitMSCK());
        }

        assertEquals(clone.getCommandLineOptions(), config.getCommandLineOptions());
        assertEquals(clone.isCopyAvroSchemaUrls(), config.isCopyAvroSchemaUrls());
        assertEquals(clone.getConnectionPoolLib(), config.getConnectionPoolLib());
        assertEquals(clone.getDataStrategy(), config.getDataStrategy());
        assertEquals(clone.isDatabaseOnly(), config.isDatabaseOnly());
        assertEquals(clone.isDumpTestData(), config.isDumpTestData());
        assertEquals(clone.getLoadTestDataFile(), config.getLoadTestDataFile());
//        assertEquals(clone.isEvaluatePartitionLocation(), config.isEvaluatePartitionLocation());

        // Filter
        assertEquals(clone.getFilter().getTblExcludeFilterPattern(), config.getFilter().getTblExcludeFilterPattern());
        assertEquals(clone.getFilter().getTblRegEx(), config.getFilter().getTblRegEx());
        assertEquals(clone.getFilter().getTblSizeLimit(), config.getFilter().getTblSizeLimit());
        assertEquals(clone.getFilter().getTblPartitionLimit(), config.getFilter().getTblPartitionLimit());

        assertEquals(clone.getDatabases(), config.getDatabases());

        // Legacy Translations
        assertEquals(clone.getLegacyTranslations().getRowSerde(), config.getLegacyTranslations().getRowSerde());

        assertEquals(clone.getDbPrefix(), config.getDbPrefix());
        assertEquals(clone.getDbRename(), config.getDbRename());
        assertEquals(clone.getDumpSource(), config.getDumpSource());
        assertEquals(clone.isExecute(), config.isExecute());

        // Hybrid
        assertEquals(clone.getHybrid().getSqlPartitionLimit(), config.getHybrid().getSqlPartitionLimit());
        assertEquals(clone.getHybrid().getSqlSizeLimit(), config.getHybrid().getSqlSizeLimit());
        assertEquals(clone.getHybrid().getExportImportPartitionLimit(), config.getHybrid().getExportImportPartitionLimit());

        // Iceberg
        assertEquals(clone.getIcebergConversion().getVersion(), config.getIcebergConversion().getVersion());
        assertEquals(clone.getIcebergConversion().getTableProperties(), config.getIcebergConversion().getTableProperties());

        // Migrate ACID
        assertEquals(clone.getMigrateACID().isOn(), config.getMigrateACID().isOn());
        assertEquals(clone.getMigrateACID().isOnly(), config.getMigrateACID().isOnly());
        assertEquals(clone.getMigrateACID().getArtificialBucketThreshold(), config.getMigrateACID().getArtificialBucketThreshold());
        assertEquals(clone.getMigrateACID().getPartitionLimit(), config.getMigrateACID().getPartitionLimit());
        assertEquals(clone.getMigrateACID().isDowngrade(), config.getMigrateACID().isDowngrade());
        assertEquals(clone.getMigrateACID().isInplace(), config.getMigrateACID().isInplace());

        // Migrate View
        assertEquals(clone.getMigrateVIEW().isOn(), config.getMigrateVIEW().isOn());

        assertEquals(clone.isMigrateNonNative(), config.isMigrateNonNative());

        // Optimizations
        assertEquals(clone.getOptimization().isSortDynamicPartitionInserts(), config.getOptimization().isSortDynamicPartitionInserts());
        assertEquals(clone.getOptimization().isSkip(), config.getOptimization().isSkip());
        assertEquals(clone.getOptimization().isAutoTune(), config.getOptimization().isAutoTune());
        assertEquals(clone.getOptimization().isCompressTextOutput(), config.getOptimization().isCompressTextOutput());
        assertEquals(clone.getOptimization().isSkipStatsCollection(), config.getOptimization().isSkipStatsCollection());
        assertEquals(clone.getOptimization().getOverrides().getFor(Environment.LEFT), config.getOptimization().getOverrides().getFor(Environment.LEFT));
        assertEquals(clone.getOptimization().getOverrides().getFor(Environment.RIGHT), config.getOptimization().getOverrides().getFor(Environment.RIGHT));
        assertEquals(clone.getOptimization().isBuildShadowStatistics(), config.getOptimization().isBuildShadowStatistics());

        assertEquals(clone.getOutputDirectory(), config.getOutputDirectory());
        assertEquals(clone.isQuiet(), config.isQuiet());
        assertEquals(clone.isReadOnly(), config.isReadOnly());
        assertEquals(clone.isNoPurge(), config.isNoPurge());
        assertEquals(clone.isReplace(), config.isReplace());
        assertEquals(clone.isResetRight(), config.isResetRight());
//        assertEquals(clone.isResetToDefaultLocation(), config.isResetToDefaultLocation());
        assertEquals(clone.isSkipFeatures(), config.isSkipFeatures());
        assertEquals(clone.isSkipLegacyTranslation(), config.isSkipLegacyTranslation());
//        assertEquals(clone.isSqlOutput(), config.isSqlOutput());
        assertEquals(clone.isSync(), config.isSync());

        // Transfer
        assertEquals(clone.getTransfer().getTransferPrefix(), config.getTransfer().getTransferPrefix());
        assertEquals(clone.getTransfer().getShadowPrefix(), config.getTransfer().getShadowPrefix());
        assertEquals(clone.getTransfer().getExportBaseDirPrefix(), config.getTransfer().getExportBaseDirPrefix());
        assertEquals(clone.getTransfer().getRemoteWorkingDirectory(), config.getTransfer().getRemoteWorkingDirectory());
        assertEquals(clone.getTransfer().getIntermediateStorage(), config.getTransfer().getIntermediateStorage());
        assertEquals(clone.getTransfer().getTargetNamespace(), config.getTransfer().getTargetNamespace());
        // Transfer Storage Migration
        assertEquals(clone.getTransfer().getStorageMigration().getDataMovementStrategy(), config.getTransfer().getStorageMigration().getDataMovementStrategy());
        assertEquals(clone.getTransfer().getStorageMigration().isDistcp(), config.getTransfer().getStorageMigration().isDistcp());
        assertEquals(clone.getTransfer().getStorageMigration().getDataFlow(), config.getTransfer().getStorageMigration().getDataFlow());
        // Transfer Warehouse
        assertEquals(clone.getTransfer().getWarehouse().getManagedDirectory(), config.getTransfer().getWarehouse().getManagedDirectory());
        assertEquals(clone.getTransfer().getWarehouse().getExternalDirectory(), config.getTransfer().getWarehouse().getExternalDirectory());

        assertEquals(clone.getOwnershipTransfer().isDatabase(), config.getOwnershipTransfer().isDatabase());
        assertEquals(clone.getOwnershipTransfer().isTable(), config.getOwnershipTransfer().isTable());

        // Translator
        assertEquals(clone.getTranslator().isForceExternalLocation(), config.getTranslator().isForceExternalLocation());
        // TODO: Need to Test the GLM's
//        assertEquals(clone.getTranslator().getGlobalLocationMap(), config.getTranslator().getGlobalLocationMap());

    }


    @Test
    public void equalsTest_101() {
        log.info("Test 01");
        HmsMirrorConfig clone = config.clone();

        // Add your assertions here
        assertEquals(config, clone);

    }

}
