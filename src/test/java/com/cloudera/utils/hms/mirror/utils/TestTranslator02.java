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
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


@Slf4j
public class TestTranslator02 extends TranslatorTestBase {

    @Before
    public void setup() throws IOException {
        log.info("Setting up TestTranslator01");
        translator = deserializeResource("/translator/testcase_02.yaml");
        HmsMirrorConfig config = ConfigTest.deserializeResource("/config/default_01.yaml");
        config.setTranslator(translator);
        // Used to prevent attempting to init connections.
        config.setLoadTestDataFile("something.yaml");

        ExecuteSessionService executeSessionService = new ExecuteSessionService();

        ConnectionPoolService connectionPoolService = new ConnectionPoolService();
        executeSessionService.setConnectionPoolService(connectionPoolService);

        ExecuteSession session = executeSessionService.createSession(ExecuteSessionService.DEFAULT, config);
        ConfigService configService = new ConfigService();
        executeSessionService.setConfigService(configService);
        executeSessionService.setSession(session);
        try {
            executeSessionService.startSession(1);
        } catch (SessionException e) {
            throw new RuntimeException(e);
        }
        warehouseService = new WarehouseService();
        warehouseService.setExecuteSessionService(executeSessionService);
//        configService.setExecuteSessionService(executeSessionService);
        translatorService = new TranslatorService();
        translatorService.setExecuteSessionService(executeSessionService);
        translatorService.setWarehouseService(warehouseService);
//        translatorService.setConfigService(configService);


    }

    @Test
    public void translateTableLocation_tbl_alt_01() {
        DBMirror dbMirror = new DBMirror();
        dbMirror.setName("tpcds_10");
        TableMirror tableMirror = new TableMirror();
        tableMirror.setName("web_sales");
        // Used to trigger an external table check.
        tableMirror.getTableDefinition(Environment.RIGHT).add("CREATE EXTERNAL TABLE");
        tableMirror.setParent(dbMirror);
        Assert.assertTrue("Couldn't validate translator configuration", translator.validate());

        String originalLoc = "hdfs://LEFT/tpcds_base_dir/web_sales";
        String expectedLoc = "hdfs://RIGHT/alt/ext/location/web_sales";
        try {
            String translatedLocation =
                    translatorService.translateTableLocation(tableMirror, originalLoc, 1, null);
            assertEquals("Table Location Failed: ", expectedLoc, translatedLocation);
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

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
            assertEquals("Table Location Failed: ", expectedLoc, translatedLocation);
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

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
            assertEquals("Table Location Failed: ", expectedLoc, translatedLocation);
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

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
            assertEquals("Table Location Failed: ", expectedLoc, translatedLocation);
        } catch (Throwable t) {
            fail("Table Location Failed: " + originalLoc + " : " + expectedLoc + " : " +
                    t.getMessage());
        }
    }

}