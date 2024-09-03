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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp_to_cdp;

import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static com.cloudera.utils.hms.mirror.MirrorConf.ALTER_DB_LOCATION_DESC;
import static com.cloudera.utils.hms.mirror.MirrorConf.ALTER_DB_MNGD_LOCATION_DESC;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=SQL",
                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.reset-to-default-location=true",
                "--hms-mirror.config.align-locations=true",
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/sql_mao_wd_da_rdl_dc"
        })
@Slf4j
public class Test_sql_mao_wd_da_rdl_dc extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SQL",
//                "-mao",
//                "-ewd", "/warehouse/external",
//                "-wd", "/warehouse/managed",
//                "-da",
//                "-rdl",
//                "--distcp",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals("Return Code Failure: " + rtn, check, rtn);
    }

    @Test
    public void sqlPairTest() {
        // Validate the SQL Pair.
        validateTableSqlPair("assorted_test_db", Environment.LEFT, "acid_02", TableUtils.STAGE_TRANSFER_DESC,
                "FROM acid_02 INSERT OVERWRITE TABLE hms_mirror_transfer_acid_02 SELECT *");
        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_02", TableUtils.STAGE_TRANSFER_DESC,
                "FROM hms_mirror_shadow_acid_02 INSERT OVERWRITE TABLE acid_02 SELECT *");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT, ALTER_DB_LOCATION_DESC,
                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/external/assorted_test_db.db\"");
        validateDBSqlPair("assorted_test_db", Environment.RIGHT, ALTER_DB_MNGD_LOCATION_DESC,
                "ALTER DATABASE assorted_test_db SET MANAGEDLOCATION \"hdfs://HOME90/warehouse/managed/assorted_test_db.db\"");
    }

    @Test
    public void dbLocationTest() {
        validateDBLocation("assorted_test_db", Environment.RIGHT,
                "hdfs://HOME90/warehouse/external/assorted_test_db.db");
        validateDBManagedLocation("assorted_test_db", Environment.RIGHT,
                "hdfs://HOME90/warehouse/managed/assorted_test_db.db");
    }

    @Test
    public void tableLocationTest() {
        validateWorkingTableLocation("assorted_test_db", "acid_02", "hms_mirror_transfer_acid_02", Environment.TRANSFER,
                "hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_02");
        validateWorkingTableLocation("assorted_test_db", "acid_02", "hms_mirror_shadow_acid_02", Environment.SHADOW,
                "hdfs://HDP50/apps/hive/warehouse/export_assorted_test_db/acid_02");
        validateTableLocation("assorted_test_db", "acid_01", Environment.RIGHT,
                null);
    }

//    @Test
//    public void phaseTest() {
//        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.SUCCESS);
//    }
//
//    @Test
//    public void issueTest() {
//        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
//                Environment.LEFT, 17);
//    }

}
