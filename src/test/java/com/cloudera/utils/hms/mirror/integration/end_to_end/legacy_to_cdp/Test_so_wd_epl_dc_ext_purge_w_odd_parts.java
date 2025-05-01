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

package com.cloudera.utils.hms.mirror.integration.end_to_end.legacy_to_cdp;

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=SCHEMA_ONLY",
//                "--hms-mirror.config.migrate-acid=true",
//                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.warehouse-plans=ext_purge_odd_parts=/finance/external-fso:/finance/managed-fso",
//                "--hms-mirror.config.warehouse-directory=/finance/managed-fso",
//                "--hms-mirror.config.external-warehouse-directory=/finance/external-fso",
//                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.read-only=true",
//                "--hms-mirror.config.sync=true",
                "--hms-mirror.config.align-locations=true",
//                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
//                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
//                "--hms-mirror.config.reset-to-default-location=true",
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.hdp2-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/legacy_cdp/so_wd_epl_dc_ext_purge_w_odd_parts"
        })
@Slf4j
public class Test_so_wd_epl_dc_ext_purge_w_odd_parts extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SCHEMA_ONLY",
//                "-wd", "/finance/managed-fso",
//                "-ewd", "/finance/external-fso",
//                "-epl",
//                "-dc",
//                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);
//
//        // Read the output and verify the results.
//        DBMirror[] resultsMirrors = getResults(outputDir,EXT_PURGE_ODD_PARTS_03);
//
//        validatePhase(resultsMirrors[0], "web_sales", PhaseState.CALCULATED_SQL);
//
//        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 18);
//        validatePartitionCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 16);
//        validateTableLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales");
//        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452035", "hdfs://HOME90/user/dstreev/datasets/alt-locations/load_web_sales/odd");
//        // ws_sold_date_sk=2451188: "hdfs://HOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188"
//        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2451188", "hdfs://HOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188");
//        // ws_sold_date_sk=2451793: "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451793"
//        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2451793", "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451793");

    @Test
    public void issueTest_01() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.RIGHT, 19);
    }

//    @Test
//    public void locationTest_01() {
//        validateTableLocation("ext_purge_odd_parts",
//                "web_sales", Environment.RIGHT,
//                "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales");
//    }

//    @Test
//    public void partitionCountTest_01() {
//        validatePartitionCount("ext_purge_odd_parts", "web_sales",
//                Environment.RIGHT, 16);
//    }

//    @Test
//    public void partitionLocationTest_01() {
//        validatePartitionLocation("ext_purge_odd_parts", "web_sales",
//                Environment.RIGHT, "ws_sold_date_sk=2452035",
//                "hdfs://HOME90/user/dstreev/datasets/alt-locations/load_web_sales/odd");
//    }

//    @Test
//    public void partitionLocationTest_02() {
//        validatePartitionLocation("ext_purge_odd_parts", "web_sales",
//                Environment.RIGHT, "ws_sold_date_sk=2451188", "hdfs://HOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188");
//    }

    //        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452035", "hdfs://HOME90/user/dstreev/datasets/alt-locations/load_web_sales/odd");
//        // ws_sold_date_sk=2451188: "hdfs://HOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188"
//        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2451188", "hdfs://HOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188");
//        // ws_sold_date_sk=2451793: "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451793"
//        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2451793", "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451793");

//    @Test
//    public void partitionLocationTest_03() {
//        validatePartitionLocation("ext_purge_odd_parts", "web_sales",
//                Environment.RIGHT, "ws_sold_date_sk=2451793", "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451793");
//    }

    @Test
    public void phaseTest_01() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = getCheckCode();
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }
}
