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

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.util.UrlUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


@Slf4j
public class TestTranslator01 extends TranslatorTestBase {

    @Before
    public void setup() throws IOException {
        log.info("Setting up TestTranslator01");
        translator = deserializeResource("/translator/testcase_01.yaml");
        HmsMirrorConfig config = ConfigTest.deserializeResource("/config/default_01.yaml");
        config.setTranslator(translator);
        ExecuteSessionService executeSessionService = new ExecuteSessionService();

        // Used to prevent attempting to init connections.
        config.setLoadTestDataFile("something.yaml");
        ConnectionPoolService connectionPoolService = new ConnectionPoolService();
        executeSessionService.setConnectionPoolService(connectionPoolService);

        ExecuteSession session = executeSessionService.createSession(null, config);
        ConfigService configService = new ConfigService();
        executeSessionService.setConfigService(configService);
        executeSessionService.setLoadedSession(session);
        try {
            executeSessionService.transitionLoadedSessionToActive(1);
        } catch (SessionException e) {
            throw new RuntimeException(e);
        }

        databaseService = new DatabaseService();
        databaseService.setExecuteSessionService(executeSessionService);

//        configService.setExecuteSessionService(executeSessionService);
        translatorService = new TranslatorService();
        translatorService.setExecuteSessionService(executeSessionService);
        translatorService.setDatabaseService(databaseService);

//        translatorService.setConfigService(configService);
    }

    @Test
    public void lastDirInUrl_01() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call";
        dir = UrlUtils.getLastDirFromUrl(dir);
        assertEquals("Directory Reduction Failed: " + dir, "call", dir);
    }

    @Test
    public void removeLastDirInUrl_01() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call";
        dir = UrlUtils.removeLastDirFromUrl(dir);
        assertEquals("Remove Last Directory Failed: " + dir, "hdfs://apps/hive/warehouse/my.db", dir);
    }

    @Test
    public void lastUrlBit_01() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call/";
        dir = UrlUtils.reduceUrlBy(dir, 1);
        assertEquals("Directory Reduction Failed: " + dir, "hdfs://apps/hive/warehouse/my.db", dir);
    }

    @Test
    public void lastUrlBit_02() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call";
        dir = UrlUtils.reduceUrlBy(dir, 2);
        assertEquals("Directory Reduction Failed: " + dir, "hdfs://apps/hive/warehouse", dir);
    }

    @Test
    public void lastUrlBit_03() {
        String dir = "hdfs://apps/hive/warehouse/my.db/warehouse";
        dir = UrlUtils.reduceUrlBy(dir, 1);
        assertEquals("Directory Reduction Failed: " + dir, "hdfs://apps/hive/warehouse/my.db", dir);
    }


//    @Test
//    public void translateDatabase_01() throws IOException {
//        Translator translator = deserializeResource("/translator/testcase_01.yaml");
//        HmsMirrorConfig hmsMirrorConfig = new HmsMirrorConfig();
//        HmsMirrorCfgService hmsMirrorCfgService = new HmsMirrorCfgService();
//        hmsMirrorCfgService.setHmsMirrorConfig(hmsMirrorConfig);
//        hmsMirrorConfig.setTranslator(translator);
//        TranslatorService translatorService = new TranslatorService();
//        translatorService.setHmsMirrorCfgService(hmsMirrorCfgService);
//
//        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
//        Assert.assertTrue("DB Rename Failed", translatorService.translateDatabase("tpcds_10").equals("tpc_ds_10"));
//    }

    @Test
    public void translateTableLocation_03() throws IOException {

        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("call_center");
        tableMirror.setParent(dbMirror);
        // Needed to help identify as EXTERNAL.
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");

        // Need by translation to get location.
        tableMirror.getTableDefinition(Environment.LEFT).add("mytable");
        tableMirror.getTableDefinition(Environment.LEFT).add("LOCATION");
        tableMirror.getTableDefinition(Environment.LEFT).add("hdfs://LEFT/apps/hive/warehouse/tpcds_09.db");

        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/apps/hive/warehouse/tpcds_09.db";
        String expectedLoc = "hdfs://RIGHT/apps/hive/warehouse/tpcds_09.db";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

    @Test
    public void translateTableLocation_consolidate_01() throws IOException {
//        Translator translator = deserializeResource("/translator/testcase_01.yaml");
//        HmsMirrorConfig config = ConfigTest.deserializeResource("/config/default_01.yaml");
//        config.setTranslator(translator);
//        ExecuteSessionService executeSessionService = new ExecuteSessionService();
//        executeSessionService.getCurrentSession().setHmsMirrorConfig(config);
////        hmsMirrorCfgService.setHmsMirrorConfig(config);
//        TranslatorService translatorService = new TranslatorService();
//        translatorService.setExecuteSessionService(executeSessionService);

        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("call_center");
        tableMirror.setParent(dbMirror);
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        String originalLoc = "hdfs://LEFT/tpcds_base_dir";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/new_location";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Assertion Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

    @Test
    public void translateTableLocation_tbl_alt_02() throws IOException {
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("call_center");
        tableMirror.setParent(dbMirror);
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");

        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir2";
        String expectedLoc = "hdfs://RIGHT/myspace/alt/ext/call_center";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

    @Test
    /*
     * Test when db translation defined with consolidation.  But the table is NOT listed.
     * This should consolidate the table to the db location in a directory named after the table.
     */
    public void translateTableLocation_tbl_alt_03() throws IOException {
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("call_center");
        tableMirror.setParent(dbMirror);
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir3";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_sales";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

    @Test
    public void translateTableLocation_tbl_alt_04() throws IOException {
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("call_center");
        tableMirror.setParent(dbMirror);
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir5/web_sales";
        String expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_sales";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }

        TableMirror tableMirror1 = new TableMirror();
        tableMirror1.setName("call_center");
        tableMirror1.setParent(dbMirror);
        tableMirror1.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        originalLoc = "hdfs://LEFT/tpcds_base_dir4/call_center2";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/call_center2";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror1, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }

        TableMirror tableMirror2 = new TableMirror();
        tableMirror2.setName("web_returns");
        tableMirror2.setParent(dbMirror);
        tableMirror2.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        originalLoc = "hdfs://LEFT/tpcds_base_dir4/web/web_returns";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_returns";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror2, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
        TableMirror tableMirror3 = new TableMirror();
        tableMirror3.setName("web_returns");
        tableMirror3.setParent(dbMirror);
        tableMirror3.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/web_returns2";
        expectedLoc = "hdfs://RIGHT/user/dstreev/datasets/tpcds_11.db/web_returns";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror3, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
        TableMirror tableMirror4 = new TableMirror();
        tableMirror4.setName("call_center");
        tableMirror4.setParent(dbMirror);
        tableMirror4.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/call_center";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_11.db/call_center";
        try {
            String translatedLocation =
                    translatorService.translateLocation(tableMirror4, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }

        log.debug("dbLocationMap:\n");
    }

//    @Test
//    public void translateTable_01() throws IOException {
//        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
//        String dbName = "tpcds_12";
//        String tblName = "call_center";
//        String tblExpectedName = "call_center_new";
//        String tblNewName = translatorService.translateTable(dbName, tblName);
//        Assert.assertTrue("Table Rename Failed " + dbName + " : " + tblName + " : " + tblNewName +
//                " : " + tblExpectedName, tblNewName.equals(tblExpectedName));
//    }

//    @Test
//    public void translateTable_02() throws IOException {
//        Translator translator = deserializeResource("/translator/testcase_01.yaml");
////        translator.setOn(true);
//        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
//        String dbName = "unknown";
//        String tblName = "my_table";
//        String tblExpectedName = "my_table";
//        String tblNewName = translator.translateTable(dbName, tblName);
//        Assert.assertTrue("Table Rename Failed " + dbName + " : " + tblName + " : " + tblNewName +
//                " : " + tblExpectedName, tblNewName.equals(tblExpectedName));
//    }

}