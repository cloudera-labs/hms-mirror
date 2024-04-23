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

import com.cloudera.utils.hms.mirror.DBMirror;
import com.cloudera.utils.hms.mirror.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.TableMirror;
import com.cloudera.utils.hms.mirror.Translator;
import com.cloudera.utils.hms.mirror.service.HmsMirrorCfgService;
import com.cloudera.utils.hms.mirror.service.TranslatorService;
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
        HmsMirrorCfgService hmsMirrorCfgService = new HmsMirrorCfgService(config);
//        hmsMirrorCfgService.setHmsMirrorConfig(config);
        translatorService = new TranslatorService();
        translatorService.setHmsMirrorCfgService(hmsMirrorCfgService);
    }

    @Test
    public void lastDirInUrl_01() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call";
        dir = Translator.getLastDirFromUrl(dir);
        assertEquals("Directory Reduction Failed: " + dir, "call", dir);
    }

    @Test
    public void removeLastDirInUrl_01() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call";
        dir = Translator.removeLastDirFromUrl(dir);
        assertEquals("Remove Last Directory Failed: " + dir, "hdfs://apps/hive/warehouse/my.db", dir);
    }

    @Test
    public void lastUrlBit_01() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call/";
        dir = Translator.reduceUrlBy(dir, 1);
        assertEquals("Directory Reduction Failed: " + dir, "hdfs://apps/hive/warehouse/my.db", dir);
    }

    @Test
    public void lastUrlBit_02() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call";
        dir = Translator.reduceUrlBy(dir, 2);
        assertEquals("Directory Reduction Failed: " + dir, "hdfs://apps/hive/warehouse", dir);
    }

    @Test
    public void lastUrlBit_03() {
        String dir = "hdfs://apps/hive/warehouse/my.db/warehouse";
        dir = Translator.reduceUrlBy(dir, 1);
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
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/apps/hive/warehouse/tpcds_09.db";
        String expectedLoc = "hdfs://RIGHT/apps/hive/warehouse/tpcds_09.db";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

    @Test
    public void translateTableLocation_consolidate_01() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        HmsMirrorConfig config = ConfigTest.deserializeResource("/config/default_01.yaml");
        config.setTranslator(translator);
        HmsMirrorCfgService hmsMirrorCfgService = new HmsMirrorCfgService(config);
//        hmsMirrorCfgService.setHmsMirrorConfig(config);
        TranslatorService translatorService = new TranslatorService();
        translatorService.setHmsMirrorCfgService(hmsMirrorCfgService);

        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("call_center");
        tableMirror.setParent(dbMirror);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        String originalLoc = "hdfs://LEFT/tpcds_base_dir";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/new_location";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
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
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir2";
        String expectedLoc = "hdfs://RIGHT/myspace/alt/ext/call_center";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror, originalLoc, 1, null);
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
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir3";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_sales";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror, originalLoc, 1, null);
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
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir5/web_sales";
        String expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_sales";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }

        TableMirror tableMirror1 = new TableMirror();
        tableMirror1.setName("call_center");
        tableMirror1.setParent(dbMirror);
        originalLoc = "hdfs://LEFT/tpcds_base_dir4/call_center2";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/call_center2";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror1, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }

        TableMirror tableMirror2 = new TableMirror();
        tableMirror2.setName("web_returns");
        tableMirror2.setParent(dbMirror);
        originalLoc = "hdfs://LEFT/tpcds_base_dir4/web/web_returns";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_returns";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror2, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
        TableMirror tableMirror3 = new TableMirror();
        tableMirror3.setName("web_returns");
        tableMirror3.setParent(dbMirror);
        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/web_returns2";
        expectedLoc = "hdfs://RIGHT/user/dstreev/datasets/tpcds_11.db/web_returns";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror3, originalLoc, 1, null);
            Assert.assertEquals("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    translatedLocation, expectedLoc, translatedLocation);
        } catch (Throwable t) {
            Assert.fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
        TableMirror tableMirror4 = new TableMirror();
        tableMirror4.setName("call_center");
        tableMirror4.setParent(dbMirror);
        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/call_center";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_11.db/call_center";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror4, originalLoc, 1, null);
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