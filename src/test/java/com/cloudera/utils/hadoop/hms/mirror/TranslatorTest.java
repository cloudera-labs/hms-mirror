/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

public class TranslatorTest {
    private static Logger LOG = LogManager.getLogger(TranslatorTest.class);

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void translateDatabase_01() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        Assert.assertTrue("DB Rename Failed", translator.translateDatabase("tpcds_10").equals("tpc_ds_10"));
    }

    @Test
    public void translateTable_01() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        String dbName = "tpcds_12";
        String tblName = "call_center";
        String tblExpectedName = "call_center_new";
        String tblNewName = translator.translateTable(dbName, tblName);
        Assert.assertTrue("Table Rename Failed " + dbName + " : " + tblName + " : " + tblNewName +
                " : " + tblExpectedName, tblNewName.equals(tblExpectedName));
    }

    @Test
    public void translateTable_02() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        String dbName = "unknown";
        String tblName = "my_table";
        String tblExpectedName = "my_table";
        String tblNewName = translator.translateTable(dbName, tblName);
        Assert.assertTrue("Table Rename Failed " + dbName + " : " + tblName + " : " + tblNewName +
                " : " + tblExpectedName, tblNewName.equals(tblExpectedName));
    }

    @Test
    public void translateTableLocation_consolidate_01() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        Config config = ConfigTest.deserializeResource("/config/default_01.yaml");
        String originalLoc = "hdfs://LEFT/tpcds_base_dir";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/new_location";
        String translatedLocation =
                translator.translateTableLocation("tpcds_10", "call_center", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));
    }

    @Test
    public void translateTableLocation_tbl_alt_02() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        Config config = ConfigTest.deserializeResource("/config/default_01.yaml");
        String originalLoc = "hdfs://LEFT/tpcds_base_dir";
        String expectedLoc = "hdfs://RIGHT/myspace/alt/ext/call_center";
        String translatedLocation =
                translator.translateTableLocation("tpcds_11", "call_center", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));
    }

    @Test
    /**
     * Test when db translation defined with consolidation.  But the table is NOT listed.
     * This should consolidate the table to the db location in a directory named after the table.
     */
    public void translateTableLocation_tbl_alt_03() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        Config config = ConfigTest.deserializeResource("/config/default_01.yaml");
        String originalLoc = "hdfs://LEFT/tpcds_base_dir";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_sales";
        String translatedLocation =
                translator.translateTableLocation("tpcds_10", "web_sales", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));
    }

    @Test
    public void translateTableLocation_tbl_alt_04() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        Config config = ConfigTest.deserializeResource("/config/default_01.yaml");
        String originalLoc = "hdfs://LEFT/tpcds_base_dir/web_sales";
        String expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_sales";
        String translatedLocation =
                translator.translateTableLocation("tpcds_10", "web_sales", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        originalLoc = "hdfs://LEFT/tpcds_base_dir/call_center2";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/call_center";
        translatedLocation =
                translator.translateTableLocation("tpcds_10", "call_center", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/web_returns";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_10.db/web_returns";
        translatedLocation =
                translator.translateTableLocation("tpcds_10", "web_returns", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/web_returns2";
        expectedLoc = "hdfs://RIGHT/user/dstreev/datasets/tpcds_11.db/web_returns";
        translatedLocation =
                translator.translateTableLocation("tpcds_11", "web_returns", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/call_center2";
        expectedLoc = "hdfs://RIGHT/warehouse/tablespace/external/hive/tpcds_11.db/call_center";
        translatedLocation =
                translator.translateTableLocation("tpcds_11", "call_center", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        LOG.debug("dbLocationMap:\n");
    }

    @Test
    public void translateTableLocation_tbl_alt_05() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_02.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        Config config = ConfigTest.deserializeResource("/config/default_01.yaml");
        String originalLoc = "hdfs://LEFT/tpcds_base_dir/web_sales";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_sales";
        String translatedLocation =
                translator.translateTableLocation("tpcds_10", "web_sales", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        originalLoc = "hdfs://LEFT/tpcds_base_dir/call_center2";
        expectedLoc = "hdfs://RIGHT/alt/ext/location/call_center2";
        translatedLocation =
                translator.translateTableLocation("tpcds_10", "call_center", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/web_returns";
        expectedLoc = "hdfs://RIGHT/alt/ext/location/web_returns";
        translatedLocation =
                translator.translateTableLocation("tpcds_10", "web_returns", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        originalLoc = "hdfs://LEFT/tpcds_base_dir/web/web_returns";
        expectedLoc = "hdfs://RIGHT/alt/ext/location/web_returns";
        translatedLocation =
                translator.translateTableLocation("tpcds_11", "web_returns", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));

        LOG.debug("dbLocationMap:\n");
    }

    @Test
    public void translateTableLocation_03() throws IOException {
        Translator translator = deserializeResource("/translator/testcase_01.yaml");
        translator.setOn(true);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());
        Config config = ConfigTest.deserializeResource("/config/default_01.yaml");
        String originalLoc = "hdfs://LEFT/apps/hive/warehouse/tpcds_09.db";
        String expectedLoc = "hdfs://RIGHT/apps/hive/warehouse/tpcds_09.db";
        String translatedLocation =
                translator.translateTableLocation("tpcds_09", "call_center", originalLoc, config);
        Assert.assertTrue("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                translatedLocation, expectedLoc.equals(translatedLocation));
    }

    @Test
    public void lastUrlBit_01() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call/";
        dir = Translator.reduceUrlBy(dir, 1);
        Assert.assertTrue("Directory Reduction Failed: " + dir, dir.equals("hdfs://apps/hive/warehouse/my.db"));
    }

    @Test
    public void lastUrlBit_02() {
        String dir = "hdfs://apps/hive/warehouse/my.db/call";
        dir = Translator.reduceUrlBy(dir, 2);
        Assert.assertTrue("Directory Reduction Failed: " + dir, dir.equals("hdfs://apps/hive/warehouse"));
    }

    @Test
    public void lastUrlBit_03() {
        String dir = "hdfs://apps/hive/warehouse/my.db/warehouse";
        dir = Translator.reduceUrlBy(dir, 1);
        Assert.assertTrue("Directory Reduction Failed: " + dir, dir.equals("hdfs://apps/hive/warehouse/my.db"));
    }

    public static Translator deserializeResource(String configResource) throws IOException, JsonMappingException {
        Translator translator = null;
        String extension = FilenameUtils.getExtension(configResource);
        ObjectMapper mapper = null;
        if ("yaml".equals(extension.toLowerCase()) || "yml".equals(extension.toLowerCase())) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else if ("json".equals(extension.toLowerCase()) || "jsn".equals(extension.toLowerCase())) {
            mapper = new ObjectMapper(new JsonFactory());
        } else {
            throw new RuntimeException(configResource + ": can't determine type by extension.  Require one of: ['yaml',yml,'json','jsn']");
        }

        // Try as a Resource (in classpath)
        URL configURL = mapper.getClass().getResource(configResource);
        if (configURL != null) {
            // Convert to String.
            String configDefinition = IOUtils.toString(configURL, "UTF-8");
            translator = mapper.readerFor(Translator.class).readValue(configDefinition);
        } else {
            // Try on Local FileSystem.
            configURL = new URL("file", null, configResource);
            if (configURL != null) {
                String configDefinition = IOUtils.toString(configURL, "UTF-8");
                translator = mapper.readerFor(Translator.class).readValue(configDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        }

        return translator;
    }

}