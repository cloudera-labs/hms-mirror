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

    private HmsMirrorConfig hmsMirrorConfig;

    @Before
    public void setUp() throws IOException {
        log.info("Setting up HmsMirrorConfigCloneTest");
        hmsMirrorConfig = ConfigTest.deserializeResource("/config/clone/full_test_01.yaml");
    }

    @Test
    public void cloneTest_01() {
        log.info("Test 01");
        HmsMirrorConfig clone = hmsMirrorConfig.clone();

        // Acceptance Criteria:
        assertEquals(clone.getAcceptance().isSilentOverride(), hmsMirrorConfig.getAcceptance().isSilentOverride());
        assertEquals(clone.getAcceptance().isBackedUpHDFS(), hmsMirrorConfig.getAcceptance().isBackedUpHDFS());
        assertEquals(clone.getAcceptance().isBackedUpMetastore(), hmsMirrorConfig.getAcceptance().isBackedUpMetastore());
        assertEquals(clone.getAcceptance().isTrashConfigured(), hmsMirrorConfig.getAcceptance().isTrashConfigured());
        assertEquals(clone.getAcceptance().isPotentialDataLoss(), hmsMirrorConfig.getAcceptance().isPotentialDataLoss());

        // Clusters
        List<Environment> environments = Arrays.asList(Environment.LEFT, Environment.RIGHT);
        for (Environment environment : environments) {
            //
            assertEquals(clone.getCluster(environment).getHcfsNamespace(), hmsMirrorConfig.getCluster(environment).getHcfsNamespace());
            assertEquals(clone.getCluster(environment).isLegacyHive(), hmsMirrorConfig.getCluster(environment).isLegacyHive());
            assertEquals(clone.getCluster(environment).isCreateIfNotExists(), hmsMirrorConfig.getCluster(environment).isCreateIfNotExists());
            assertEquals(clone.getCluster(environment).isHdpHive3(), hmsMirrorConfig.getCluster(environment).isHdpHive3());
            assertEquals(clone.getCluster(environment).isCreateIfNotExists(), hmsMirrorConfig.getCluster(environment).isCreateIfNotExists());
            assertEquals(clone.getCluster(environment).isEnableAutoColumnStats(), hmsMirrorConfig.getCluster(environment).isEnableAutoColumnStats());
            assertEquals(clone.getCluster(environment).isEnableAutoTableStats(), hmsMirrorConfig.getCluster(environment).isEnableAutoTableStats());
            // Hive Server 2
            if (isNull(hmsMirrorConfig.getCluster(environment).getHiveServer2())) {
                assertNull(clone.getCluster(environment).getHiveServer2());
            } else {
                assertEquals(clone.getCluster(environment).getHiveServer2().getUri(), hmsMirrorConfig.getCluster(environment).getHiveServer2().getUri());
                assertEquals(clone.getCluster(environment).getHiveServer2().isDisconnected(), hmsMirrorConfig.getCluster(environment).getHiveServer2().isDisconnected());
                assertEquals(clone.getCluster(environment).getHiveServer2().getDriverClassName(), hmsMirrorConfig.getCluster(environment).getHiveServer2().getDriverClassName());
                assertEquals(clone.getCluster(environment).getHiveServer2().getJarFile(), hmsMirrorConfig.getCluster(environment).getHiveServer2().getJarFile());
                // Connection Properties
                assertEquals(clone.getCluster(environment).getHiveServer2().getConnectionProperties().get("user"), hmsMirrorConfig.getCluster(environment).getHiveServer2().getConnectionProperties().get("user"));
                assertEquals(clone.getCluster(environment).getHiveServer2().getConnectionProperties().get("password"), hmsMirrorConfig.getCluster(environment).getHiveServer2().getConnectionProperties().get("password"));

            }

            // Metastore
            if (isNull(hmsMirrorConfig.getCluster(environment).getMetastoreDirect())) {
                assertNull(clone.getCluster(environment).getMetastoreDirect());
            } else {
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getUri(), hmsMirrorConfig.getCluster(environment).getMetastoreDirect().getUri());
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getType(), hmsMirrorConfig.getCluster(environment).getMetastoreDirect().getType());
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getInitSql(), hmsMirrorConfig.getCluster(environment).getMetastoreDirect().getInitSql());
                // Connection Properties
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("user"), hmsMirrorConfig.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("user"));
                assertEquals(clone.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("password"), hmsMirrorConfig.getCluster(environment).getMetastoreDirect().getConnectionProperties().get("password"));
            }

            assertEquals(clone.getCluster(environment).getPartitionDiscovery().isAuto(), hmsMirrorConfig.getCluster(environment).getPartitionDiscovery().isAuto());
            assertEquals(clone.getCluster(environment).getPartitionDiscovery().isInitMSCK(), hmsMirrorConfig.getCluster(environment).getPartitionDiscovery().isInitMSCK());
        }

        assertEquals(clone.getCommandLineOptions(), hmsMirrorConfig.getCommandLineOptions());
        assertEquals(clone.isCopyAvroSchemaUrls(), hmsMirrorConfig.isCopyAvroSchemaUrls());
        assertEquals(clone.getConnectionPoolLib(), hmsMirrorConfig.getConnectionPoolLib());
        assertEquals(clone.getDataStrategy(), hmsMirrorConfig.getDataStrategy());
        assertEquals(clone.isDatabaseOnly(), hmsMirrorConfig.isDatabaseOnly());
        assertEquals(clone.isDumpTestData(), hmsMirrorConfig.isDumpTestData());
        assertEquals(clone.getLoadTestDataFile(), hmsMirrorConfig.getLoadTestDataFile());
//        assertEquals(clone.isEvaluatePartitionLocation(), hmsMirrorConfig.isEvaluatePartitionLocation());

        // Filter
        assertEquals(clone.getFilter().getTblExcludeFilterPattern(), hmsMirrorConfig.getFilter().getTblExcludeFilterPattern());
        assertEquals(clone.getFilter().getTblRegEx(), hmsMirrorConfig.getFilter().getTblRegEx());
        assertEquals(clone.getFilter().getTblSizeLimit(), hmsMirrorConfig.getFilter().getTblSizeLimit());
        assertEquals(clone.getFilter().getTblPartitionLimit(), hmsMirrorConfig.getFilter().getTblPartitionLimit());

        assertEquals(clone.getDatabases(), hmsMirrorConfig.getDatabases());

        // Legacy Translations
        assertEquals(clone.getLegacyTranslations().getRowSerde(), hmsMirrorConfig.getLegacyTranslations().getRowSerde());

        assertEquals(clone.getDbPrefix(), hmsMirrorConfig.getDbPrefix());
        assertEquals(clone.getDbRename(), hmsMirrorConfig.getDbRename());
        assertEquals(clone.getDumpSource(), hmsMirrorConfig.getDumpSource());
        assertEquals(clone.isExecute(), hmsMirrorConfig.isExecute());

        // Hybrid
        assertEquals(clone.getHybrid().getSqlPartitionLimit(), hmsMirrorConfig.getHybrid().getSqlPartitionLimit());
        assertEquals(clone.getHybrid().getSqlSizeLimit(), hmsMirrorConfig.getHybrid().getSqlSizeLimit());
        assertEquals(clone.getHybrid().getExportImportPartitionLimit(), hmsMirrorConfig.getHybrid().getExportImportPartitionLimit());

        // Iceberg
        assertEquals(clone.getIcebergConfig().getVersion(), hmsMirrorConfig.getIcebergConfig().getVersion());
        assertEquals(clone.getIcebergConfig().getTableProperties(), hmsMirrorConfig.getIcebergConfig().getTableProperties());

        // Migrate ACID
        assertEquals(clone.getMigrateACID().isOn(), hmsMirrorConfig.getMigrateACID().isOn());
        assertEquals(clone.getMigrateACID().isOnly(), hmsMirrorConfig.getMigrateACID().isOnly());
        assertEquals(clone.getMigrateACID().getArtificialBucketThreshold(), hmsMirrorConfig.getMigrateACID().getArtificialBucketThreshold());
        assertEquals(clone.getMigrateACID().getPartitionLimit(), hmsMirrorConfig.getMigrateACID().getPartitionLimit());
        assertEquals(clone.getMigrateACID().isDowngrade(), hmsMirrorConfig.getMigrateACID().isDowngrade());
        assertEquals(clone.getMigrateACID().isInplace(), hmsMirrorConfig.getMigrateACID().isInplace());

        // Migrate View
        assertEquals(clone.getMigrateVIEW().isOn(), hmsMirrorConfig.getMigrateVIEW().isOn());

        assertEquals(clone.isMigrateNonNative(), hmsMirrorConfig.isMigrateNonNative());

        // Optimizations
        assertEquals(clone.getOptimization().isSortDynamicPartitionInserts(), hmsMirrorConfig.getOptimization().isSortDynamicPartitionInserts());
        assertEquals(clone.getOptimization().isSkip(), hmsMirrorConfig.getOptimization().isSkip());
        assertEquals(clone.getOptimization().isAutoTune(), hmsMirrorConfig.getOptimization().isAutoTune());
        assertEquals(clone.getOptimization().isCompressTextOutput(), hmsMirrorConfig.getOptimization().isCompressTextOutput());
        assertEquals(clone.getOptimization().isSkipStatsCollection(), hmsMirrorConfig.getOptimization().isSkipStatsCollection());
        assertEquals(clone.getOptimization().getOverrides().getLeft(), hmsMirrorConfig.getOptimization().getOverrides().getLeft());
        assertEquals(clone.getOptimization().getOverrides().getRight(), hmsMirrorConfig.getOptimization().getOverrides().getRight());
        assertEquals(clone.getOptimization().isBuildShadowStatistics(), hmsMirrorConfig.getOptimization().isBuildShadowStatistics());

        assertEquals(clone.getOutputDirectory(), hmsMirrorConfig.getOutputDirectory());
        assertEquals(clone.isQuiet(), hmsMirrorConfig.isQuiet());
        assertEquals(clone.isReadOnly(), hmsMirrorConfig.isReadOnly());
        assertEquals(clone.isNoPurge(), hmsMirrorConfig.isNoPurge());
        assertEquals(clone.isReplace(), hmsMirrorConfig.isReplace());
        assertEquals(clone.isResetRight(), hmsMirrorConfig.isResetRight());
//        assertEquals(clone.isResetToDefaultLocation(), hmsMirrorConfig.isResetToDefaultLocation());
        assertEquals(clone.isSkipFeatures(), hmsMirrorConfig.isSkipFeatures());
        assertEquals(clone.isSkipLegacyTranslation(), hmsMirrorConfig.isSkipLegacyTranslation());
//        assertEquals(clone.isSqlOutput(), hmsMirrorConfig.isSqlOutput());
        assertEquals(clone.isSync(), hmsMirrorConfig.isSync());

        // Transfer
        assertEquals(clone.getTransfer().getTransferPrefix(), hmsMirrorConfig.getTransfer().getTransferPrefix());
        assertEquals(clone.getTransfer().getShadowPrefix(), hmsMirrorConfig.getTransfer().getShadowPrefix());
        assertEquals(clone.getTransfer().getExportBaseDirPrefix(), hmsMirrorConfig.getTransfer().getExportBaseDirPrefix());
        assertEquals(clone.getTransfer().getRemoteWorkingDirectory(), hmsMirrorConfig.getTransfer().getRemoteWorkingDirectory());
        assertEquals(clone.getTransfer().getIntermediateStorage(), hmsMirrorConfig.getTransfer().getIntermediateStorage());
        assertEquals(clone.getTransfer().getTargetNamespace(), hmsMirrorConfig.getTransfer().getTargetNamespace());
        // Transfer Storage Migration
        assertEquals(clone.getTransfer().getStorageMigration().getDataMovementStrategy(), hmsMirrorConfig.getTransfer().getStorageMigration().getDataMovementStrategy());
        assertEquals(clone.getTransfer().getStorageMigration().isDistcp(), hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp());
        assertEquals(clone.getTransfer().getStorageMigration().getDataFlow(), hmsMirrorConfig.getTransfer().getStorageMigration().getDataFlow());
        // Transfer Warehouse
        assertEquals(clone.getTransfer().getWarehouse().getManagedDirectory(), hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory());
        assertEquals(clone.getTransfer().getWarehouse().getExternalDirectory(), hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory());

        assertEquals(clone.getOwnershipTransfer().isDatabase(), hmsMirrorConfig.getOwnershipTransfer().isDatabase());
        assertEquals(clone.getOwnershipTransfer().isTable(), hmsMirrorConfig.getOwnershipTransfer().isTable());

        // Translator
        assertEquals(clone.getTranslator().isForceExternalLocation(), hmsMirrorConfig.getTranslator().isForceExternalLocation());
        // TODO: Need to Test the GLM's
//        assertEquals(clone.getTranslator().getGlobalLocationMap(), hmsMirrorConfig.getTranslator().getGlobalLocationMap());

    }


    @Test
    public void equalsTest_101() {
        log.info("Test 01");
        HmsMirrorConfig clone = hmsMirrorConfig.clone();

        // Add your assertions here
        assertEquals(hmsMirrorConfig, clone);

    }

}
