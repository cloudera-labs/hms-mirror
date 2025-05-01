/*
 * Copyright (c) 2022-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.utils;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.mirror.util.SerializationUtils;
import com.cloudera.utils.hms.util.UrlUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.inject.Inject;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.AssertionErrors.assertTrue;

@ExtendWith(MockitoExtension.class)
@Slf4j
public class TestTranslator01 extends TranslatorTestBase {
    private static final String LEFT_HDFS = "hdfs://LEFT";
    private static final String RIGHT_HDFS = "hdfs://RIGHT";
    private static final String TEST_DB_NAME = "tpcds_10";
    private static final String TEST_TABLE_NAME = "call_center";
    private static final String TRANSLATOR_CONFIG = "/translator/testcase_01.yaml";
    private static final String DEFAULT_CONFIG = "/config/default_01.yaml";

    @BeforeEach
    public void setup1() throws IOException {
        log.info("Setting up TestTranslator01");
    }

    @Override
    protected void initializeConfig() throws IOException {
        translator = deserializeResource(TRANSLATOR_CONFIG);

        config = ConfigTest.deserializeResource(DEFAULT_CONFIG);
        config.setTranslator(translator);
        config.setLoadTestDataFile("something.yaml");

    }

    private TableMirror createTestTableMirror() {
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName(TEST_DB_NAME);
        
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName(TEST_TABLE_NAME);
        tableMirror.setParent(dbMirror);
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        return tableMirror;
    }

    private void assertLocationTranslation(String originalLoc, String expectedLoc, 
                                         TableMirror tableMirror) {
        try {
            String translatedLocation = translatorService.translateTableLocation(
                tableMirror, originalLoc, 1, null);
            assertEquals(expectedLoc, translatedLocation, "Table Location Failed");
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                t.getMessage());
        }
    }

    /**
     * Tests for URL manipulation utility functions
     */
    @Test
    public void testUrlManipulationFunctions() {
        // Test getLastDirFromUrl function
        String testUrl = "hdfs://apps/hive/warehouse/my.db/call";
        assertEquals("call", UrlUtils.getLastDirFromUrl(testUrl));
        
        // Test removeLastDirFromUrl function
        assertEquals("hdfs://apps/hive/warehouse/my.db", 
            UrlUtils.removeLastDirFromUrl(testUrl));
        
        // Test reduceUrlBy with different levels
        assertEquals("hdfs://apps/hive/warehouse", 
            UrlUtils.reduceUrlBy(testUrl, 2));
            
        // Test with trailing slash
        String urlWithSlash = "hdfs://apps/hive/warehouse/my.db/call/";
        assertEquals("hdfs://apps/hive/warehouse/my.db", 
            UrlUtils.reduceUrlBy(urlWithSlash, 1));
            
        // Test with different last directory name
        String warehouseUrl = "hdfs://apps/hive/warehouse/my.db/warehouse";
        assertEquals("hdfs://apps/hive/warehouse/my.db", 
            UrlUtils.reduceUrlBy(warehouseUrl, 1));
    }
    
    /**
     * Tests for basic table location translation
     */
    @Test
    public void testBasicTableLocationTranslation() throws IOException {
        TableMirror tableMirror = createTestTableMirror();
        assertTrue("Couldn't validate translator configuration", translator.validate());
    
        // Test basic location translations
        assertLocationTranslation(
            LEFT_HDFS + "/tpcds_base_dir",
            RIGHT_HDFS + "/alt/ext/location/new_location",
            tableMirror
        );
    
        assertLocationTranslation(
            LEFT_HDFS + "/tpcds_base_dir2",
            RIGHT_HDFS + "/myspace/alt/ext/call_center",
            tableMirror
        );
    }
    
    /**
     * Test for standard HDFS namespace translation
     */
    @Test
    public void testStandardHdfsNamespaceTranslation() throws IOException {
        // Create table mirror with LEFT location definition
        TableMirror tableMirror = createTestTableMirror();
        tableMirror.getTableDefinition(Environment.LEFT).add("mytable");
        tableMirror.getTableDefinition(Environment.LEFT).add("LOCATION");
        tableMirror.getTableDefinition(Environment.LEFT).add("hdfs://LEFT/apps/hive/warehouse/tpcds_09.db");
        
        assertTrue("Couldn't validate translator configuration", translator.validate());
    
        // Test standard location translation (namespace substitution)
        assertLocationTranslation(
            "hdfs://LEFT/apps/hive/warehouse/tpcds_09.db",
            "hdfs://RIGHT/apps/hive/warehouse/tpcds_09.db",
            tableMirror
        );
    }
    
    /**
     * Test for table location translation consolidation
     */
    @Test
    public void testTableLocationConsolidation() throws IOException {
        // Create test table mirror
        TableMirror tableMirror = createTestTableMirror();
        assertTrue("Couldn't validate translator configuration", translator.validate());
    
        // Test basic consolidation
        assertLocationTranslation(
            "hdfs://LEFT/tpcds_base_dir",
            "hdfs://RIGHT/alt/ext/location/new_location",
            tableMirror
        );
        
        // Test when db translation defined with consolidation but table is NOT listed
        assertLocationTranslation(
            "hdfs://LEFT/tpcds_base_dir3",
            "hdfs://RIGHT/alt/ext/location/web_sales",
            tableMirror
        );
    }
    
    /**
     * Test for multiple table location translation patterns
     */
    @Test
    public void testMultipleTableLocationPatterns() throws IOException {
        // Setup common test objects
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName(TEST_DB_NAME);
        TableMirror callCenterTable = new TableMirror();
        callCenterTable.setName("call_center");
        callCenterTable.setParent(dbMirror);
        callCenterTable.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        
        assertTrue("Couldn't validate translator configuration", translator.validate());
        
        // Test warehouse path pattern with web_sales
        assertLocationTranslation(
            "hdfs://LEFT/tpcds_base_dir5/web_sales",
            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_sales",
            callCenterTable
        );
        
        // Test base directory 4 with call_center2
        assertLocationTranslation(
            "hdfs://LEFT/tpcds_base_dir4/call_center2",
            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/call_center2",
            callCenterTable
        );
        
        // Test nested paths with web_returns
        TableMirror webReturnsTable = new TableMirror();
        webReturnsTable.setName("web_returns");
        webReturnsTable.setParent(dbMirror);
        webReturnsTable.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        
        assertLocationTranslation(
            "hdfs://LEFT/tpcds_base_dir4/web/web_returns",
            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_returns",
            webReturnsTable
        );
        
        // Test web returns path with user directory
        assertLocationTranslation(
            "hdfs://LEFT/tpcds_base_dir/web/web_returns2",
            "hdfs://RIGHT/user/dstreev/datasets/tpcds_11.db/web_returns",
            webReturnsTable
        );
        
        // Test call_center path with different database
        assertLocationTranslation(
            "hdfs://LEFT/tpcds_base_dir/web/call_center",
            "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_11.db/call_center",
            callCenterTable
        );
    }
}