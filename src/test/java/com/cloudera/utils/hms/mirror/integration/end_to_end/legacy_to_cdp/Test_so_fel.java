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
//                "--hms-mirror.config.data-strategy=SCHEMA_ONLY",
//                "--hms-mirror.config.migrate-acid=true",
//                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
//                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.read-only=true",
//                "--hms-mirror.config.sync=true",
                "--hms-mirror.config.force-external-location=true",
//                "--hms-mirror.config.evaluate-partition-location=true",
//                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
//                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
//                "--hms-mirror.config.reset-to-default-location=true",
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.hdp2-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/legacy_cdp/so_fel"
        })
@Slf4j
public class Test_so_fel extends E2EBaseTest {
    //        String[] args = new String[]{
//                "-fel",
//                "-dc",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", HDP2_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
//
//        // Read the output and verify the results.
//        DBMirror[] resultsMirrors = getResults(outputDir,ASSORTED_TBLS_04);
//
//        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.CALCULATED_SQL);
//        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.CALCULATED_SQL);
//        validatePhase(resultsMirrors[0], "legacy_mngd_01", PhaseState.CALCULATED_SQL);
//
//        validateTableIssueCount(resultsMirrors[0], "ext_part_01", Environment.RIGHT, 2);
//        validateTableLocation(resultsMirrors[0], "ext_part_01", Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");

    @Test
    public void issueTest_01() {
        validateTableIssueCount("assorted_test_db", "ext_part_01",
                Environment.RIGHT, 2);
    }

    @Test
    public void locationTest_01() {
        validateTableLocation("assorted_test_db",
                "ext_part_01", Environment.RIGHT,
                "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");
    }

    @Test
    public void phaseTest_01() {
        validatePhase("assorted_test_db", "ext_part_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void phaseTest_02() {
        validatePhase("assorted_test_db", "legacy_mngd_01", PhaseState.CALCULATED_SQL);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

}
