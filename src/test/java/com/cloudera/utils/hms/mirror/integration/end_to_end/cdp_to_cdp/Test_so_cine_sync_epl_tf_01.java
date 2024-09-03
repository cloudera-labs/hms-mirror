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

import com.cloudera.utils.hms.mirror.PhaseState;
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
import static com.cloudera.utils.hms.util.TableUtils.REPAIR_DESC;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.create-if-not-exist=true",
//                "--hms-mirror.config.evaluate-partition-location=tru",
//                "--hms-mirror.config.align-locations=true",
                "--hms-mirror.config.table-filter=web_sales",
                "--hms-mirror.conversion.test-filename=/test_data/exists_parts_02.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/so_cine_sync_epl_tf_01"
        })
@Slf4j
public class Test_so_cine_sync_epl_tf_01 extends E2EBaseTest {
//        String[] args = new String[]{
//                "-d", "SCHEMA_ONLY"
//                , "-cine"
//                , "-epl"
//                , "-sync"
//                , "-tf", "web_sales"
//                , "-ltd", EXISTS_PARTS_08
//                , "-cfg", CDP_CDP
//                , "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

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
        validateTableSqlPair("ext_purge_odd_parts", Environment.LEFT, "web_sales", REPAIR_DESC,
                "MSCK REPAIR TABLE web_sales");
//        validateTableSqlPair("assorted_test_db", Environment.RIGHT, "acid_01", TableUtils.STAGE_TRANSFER_DESC,
//                "FROM hms_mirror_shadow_acid_01 INSERT OVERWRITE TABLE acid_01 SELECT *");
//        validateDBSqlPair("assorted_test_db", Environment.RIGHT, ALTER_DB_LOCATION_DESC,
//                "ALTER DATABASE assorted_test_db SET LOCATION \"hdfs://HOME90/warehouse/external/assorted_test_db.db\"");
        validateDBSqlPair("ext_purge_odd_parts", Environment.RIGHT, ALTER_DB_LOCATION_DESC,
                "ALTER DATABASE ext_purge_odd_parts SET LOCATION \"hdfs://HOME90/apps/hive/warehouse/ext_purge_odd_parts.db\"");
    }

    @Test
    public void dbLocationTest() {
        validateDBLocation("ext_purge_odd_parts", Environment.RIGHT,
                "hdfs://HOME90/apps/hive/warehouse/ext_purge_odd_parts.db");
//        validateDBManagedLocation("ext_purge_odd_parts", Environment.RIGHT,
//                "hdfs://HOME90/warehouse/managed/assorted_test_db.db");
    }

    @Test
    public void tableLocationTest() {
//        validateWorkingTableLocation("assorted_test_db", "acid_01", "hms_mirror_transfer_acid_01", Environment.TRANSFER,
//                "s3a://my_is_bucket/hms_mirror_working/[0-9]{8}_[0-9]{6}/assorted_test_db/acid_01");
//        validateWorkingTableLocation("assorted_test_db", "acid_01", "hms_mirror_shadow_acid_01", Environment.SHADOW,
//                "s3a://my_is_bucket/hms_mirror_working/[0-9]{8}_[0-9]{6}/assorted_test_db/acid_01");
        validateTableLocation("ext_purge_odd_parts", "web_sales", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales");
    }

    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.SUCCESS);
    }

    @Test
    public void issueTest() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.RIGHT, 2);
    }

}
