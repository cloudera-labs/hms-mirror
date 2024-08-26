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

package com.cloudera.utils.hms.mirror.integration.end_to_end.cdp;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=STORAGE_MIGRATION",
                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
//                "--hms-mirror.config.migrate-acid=4",
                "--hms-mirror.config.warehouse-plans=assorted_test_db=/warehouse/external_tables:/warehouse/managed_tables",
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_smn_wd_dc"
        })
@Slf4j
public class Test_sm_smn_wd_dc extends E2EBaseTest {
//        String[] args = new String[]{
//                "-d", "STORAGE_MIGRATION",
//                "-smn", TARGET_NAMESPACE,
//                "-wd", "/warehouse/managed_tables",
//                "-ewd", "/warehouse/external_tables",
//                "--distcp",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        int check = 0; // because 3 of the tables are acid.
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);
//
//
//        // Read the output and verify the results.
//        DBMirror[] resultsMirrors = getResults(outputDir,ASSORTED_TBLS_04);
//
//        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.SUCCESS);
//        validateTableIssueCount(resultsMirrors[0], "ext_part_01", Environment.LEFT, 441);
//
//        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "ext_part_01", "Alter Table Location",
//                "ALTER TABLE ext_part_01 SET LOCATION \"s3a://my_cs_bucket/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01\"")) {
//            fail("Alter Table Location not found");
//        }
//
//    }

//    @Test
//    public void issueTest() {
//        validateTableIssueCount("assorted_test_db", "ext_part_01",
//                Environment.LEFT, 882);
//    }

//    @Test
//    public void phaseTest() {
//        validatePhase("assorted_test_db", "ext_part_01", PhaseState.SUCCESS);
//    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long actual = getReturnCode();
        // Verify the return code.
        long expected = 0L;
//        getCheckCode(MessageCode.DISTCP_REQUIRES_EPL);

        assertEquals("Return Code Failure: ", expected, actual);

    }

    @Test
    public void validateSqlTest() {
        if (!validateSqlPair("assorted_test_db", Environment.LEFT, "ext_part_01", "Alter Table Location",
                "ALTER TABLE ext_part_01 SET LOCATION \"s3a://my_cs_bucket/warehouse/external_tables/assorted_test_db.db/ext_part_01\"")) {
            fail("Alter Table Location not found");
        }
    }

}
