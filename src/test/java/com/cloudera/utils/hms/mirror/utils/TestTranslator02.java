/*
 * Copyright (c) 2022-2024. Cloudera, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.utils.hms.mirror.utils;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import com.cloudera.utils.hms.mirror.util.SerializationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.test.util.AssertionErrors.assertTrue;

/**
 * Integration test class for validating table location translation logic.
 * <p>
 * This test exercises the TranslatorService's ability to properly
 * convert input HDFS resource locations according to configuration
 * and various table scenarios.
 */
@Slf4j
public class TestTranslator02 extends TranslatorTestBase {

    @BeforeEach
    public void setup1() throws IOException {
        log.info("Setting up TestTranslator01");
    }

    @Override
    protected void initializeConfig() throws IOException {
        translator = deserializeResource("/translator/testcase_02.yaml");

        config = ConfigTest.deserializeResource("/config/default_01.yaml");
        config.setTranslator(translator);
        config.setLoadTestDataFile("something.yaml");

    }

    /**
     * Verifies that the table location is correctly translated
     * for the "web_sales" table with an alternative location mapping.
     */
    @Test
    public void translateTableLocation_tbl_alt_01() {
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");

        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("web_sales");
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        tableMirror.setParent(dbMirror);

        assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir/web_sales";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_sales";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror, originalLoc, 1, null);
            assertEquals(expectedLoc, translatedLocation, "Table Location Failed: ");
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " + t.getMessage());
        }
    }

    /**
     * Validates alternate table location translation for the "call_center" table.
     */
    @Test
    public void translateTableLocation_tbl_alt_02() {
        DBMirror dbMirror = new DBMirror();

        TableMirror tableMirror1 = new TableMirror();
        tableMirror1.setName("call_center");
        tableMirror1.setParent(dbMirror);
        tableMirror1.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");

        String originalLoc = "hdfs://LEFT/tpcds_base_dir/call_center2";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/call_center2";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror1, originalLoc, 1, null);
            assertEquals(expectedLoc, translatedLocation, "Table Location Failed: ");
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " + t.getMessage());
        }
    }

    /**
     * Validates alternate table location translation for "web_returns" table with a different original HDFS path.
     */
    @Test
    public void translateTableLocation_tbl_alt_03() {
        DBMirror dbMirror = new DBMirror();

        TableMirror tableMirror2 = new TableMirror();
        tableMirror2.setName("web_returns");
        tableMirror2.setParent(dbMirror);
        tableMirror2.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");

        String originalLoc = "hdfs://LEFT/tpcds_base_dir2/web/web_returns";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_returns";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror2, originalLoc, 1, null);
            assertEquals(expectedLoc, translatedLocation, "Table Location Failed: ");
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " + t.getMessage());
        }
    }

    /**
     * Duplicate of tbl_alt_03; checks idempotence or stability of translation for similar input.
     */
    @Test
    public void translateTableLocation_tbl_alt_04() {
        DBMirror dbMirror = new DBMirror();

        TableMirror tableMirror3 = new TableMirror();
        tableMirror3.setName("web_returns");
        tableMirror3.setParent(dbMirror);
        tableMirror3.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");

        String originalLoc = "hdfs://LEFT/tpcds_base_dir2/web/web_returns";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_returns";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror3, originalLoc, 1, null);
            assertEquals(expectedLoc, translatedLocation, "Table Location Failed: ");
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " + t.getMessage());
        }
    }
}