package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.domain.Cluster;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class QueryDefinitionsServiceTest {

    /**
     * Test case when the environment exists in the queryDefinitionsMap cache.
     */
//    @Test
//    void testGetQueryDefinitions_CachedEnvironment() {
//        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
//        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);
//
//        QueryDefinitions cachedQueryDefinitions = new QueryDefinitions();
//        queryDefinitionsService.queryDefinitionsMap.put(Environment.LEFT, cachedQueryDefinitions);
//
//        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);
//
//        assertNotNull(result);
//        assertEquals(cachedQueryDefinitions, result, "QueryDefinitions from cache should match.");
//    }

    /**
     * Test case when the environment refers to a cluster without a metastoreDirect configuration.
     */
    @Test
    void testGetQueryDefinitions_ClusterWithoutMetastore() {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.MYSQL;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);
//        DBStore mockMetastoreDirect = mock(DBStore.class);

//        lenient().when(mockMetastoreDirect.getType()).thenReturn(type);

//        when(mockCluster.getMetastoreDirect()).thenReturn(mockMetastoreDirect);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        DBStore mockDBStore = mock(DBStore.class);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
//        when(mockCluster.getMetastoreDirect()).thenReturn(mockDBStore);
        when(mockDBStore.getType()).thenReturn(DBStore.DB_TYPE.MYSQL);

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

//        URL mockURL = this.getClass().getResource("/"+type.toString()+"/metastoreDirect.yaml");
//        assertNotNull(mockURL, "Resource file for "+type.toString()+"/metastoreDirect.yaml must exist");

//        String yamlContent = IOUtils.toString(mockURL, StandardCharsets.UTF_8);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNull(result, "QueryDefinitions should be null as the cluster lacks metastoreDirect configuration.");
    }

    /**
     * Test case when the YAML configuration for the metastore can be loaded successfully.
     */
    @Test
    void testGetQueryDefinitions_ValidYAMLMYSQLConfig() throws Exception {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.MYSQL;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        DBStore mockDBStore = mock(DBStore.class);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        when(mockCluster.getMetastoreDirect()).thenReturn(mockDBStore);
        when(mockDBStore.getType()).thenReturn(type);

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

        URL mockURL = this.getClass().getResource("/"+type.toString()+"/metastoreDirect.yaml");
        assertNotNull(mockURL, "Resource file for "+type.toString()+"/metastoreDirect.yaml must exist");

        String yamlContent = IOUtils.toString(mockURL, StandardCharsets.UTF_8);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNotNull(result, "QueryDefinitions must be loaded successfully from YAML.");
        assertNotNull(result.getQueryDefinition("part_locations"), "Loaded YAML content must have valid query for: 'part_locations'");
        assertNotNull(result.getQueryDefinition("database_partition_locations"), "Loaded YAML content must have valid query for: 'database_partition_locations'");
        assertNotNull(result.getQueryDefinition("database_table_locations"), "Loaded YAML content must have valid query for: 'database_table_locations'");
    }

    @Test
    void testGetQueryDefinitions_ValidYAMLPostgreSConfig() throws Exception {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.POSTGRES;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        DBStore mockDBStore = mock(DBStore.class);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        when(mockCluster.getMetastoreDirect()).thenReturn(mockDBStore);
        when(mockDBStore.getType()).thenReturn(type);

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

        URL mockURL = this.getClass().getResource("/"+type.toString()+"/metastoreDirect.yaml");
        assertNotNull(mockURL, "Resource file for "+type.toString()+"/metastoreDirect.yaml must exist");

        String yamlContent = IOUtils.toString(mockURL, StandardCharsets.UTF_8);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNotNull(result, "QueryDefinitions must be loaded successfully from YAML.");
        assertNotNull(result.getQueryDefinition("part_locations"), "Loaded YAML content must have valid query for: 'part_locations'");
        assertNotNull(result.getQueryDefinition("database_partition_locations"), "Loaded YAML content must have valid query for: 'database_partition_locations'");
        assertNotNull(result.getQueryDefinition("database_table_locations"), "Loaded YAML content must have valid query for: 'database_table_locations'");
    }

    @Test
    void testGetQueryDefinitions_ValidYAMLOracleConfig() throws Exception {
        DBStore.DB_TYPE type = DBStore.DB_TYPE.ORACLE;
        ExecuteSessionService mockExecuteSessionService = mock(ExecuteSessionService.class);
        HmsMirrorConfig mockHmsMirrorConfig = mock(HmsMirrorConfig.class);
        Cluster mockCluster = mock(Cluster.class);

        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        DBStore mockDBStore = mock(DBStore.class);
        ExecuteSession mockExecuteSession = mock(ExecuteSession.class);

        when(mockExecuteSession.getConfig()).thenReturn(mockHmsMirrorConfig);
        when(mockExecuteSessionService.getSession()).thenReturn(mockExecuteSession);
        when(mockHmsMirrorConfig.getCluster(Environment.LEFT)).thenReturn(mockCluster);
        when(mockCluster.getMetastoreDirect()).thenReturn(mockDBStore);
        when(mockDBStore.getType()).thenReturn(type);

        QueryDefinitionsService queryDefinitionsService = new QueryDefinitionsService(mockExecuteSessionService);

        URL mockURL = this.getClass().getResource("/"+type.toString()+"/metastoreDirect.yaml");
        assertNotNull(mockURL, "Resource file for "+type.toString()+"/metastoreDirect.yaml must exist");

        String yamlContent = IOUtils.toString(mockURL, StandardCharsets.UTF_8);

        QueryDefinitions result = queryDefinitionsService.getQueryDefinitions(Environment.LEFT);

        assertNotNull(result, "QueryDefinitions must be loaded successfully from YAML.");
        assertNotNull(result.getQueryDefinition("part_locations"), "Loaded YAML content must have valid query for: 'part_locations'");
        assertNotNull(result.getQueryDefinition("database_partition_locations"), "Loaded YAML content must have valid query for: 'database_partition_locations'");
        assertNotNull(result.getQueryDefinition("database_table_locations"), "Loaded YAML content must have valid query for: 'database_table_locations'");
    }

    /**
     * Test case when the YAML configuration file is missing.
     */
}