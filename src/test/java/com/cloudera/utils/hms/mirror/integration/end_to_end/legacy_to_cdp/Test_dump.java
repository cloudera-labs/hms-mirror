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
                "--hms-mirror.config.comment='Test the DUMP Strategy'",
                "--hms-mirror.config.data-strategy=DUMP",
//                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
//                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.reset-to-default-location=true",
//                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/assorted_tbls_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.hdp2-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/legacy_cdp/dump"
        })
@Slf4j
public class Test_dump extends E2EBaseTest {
    //        String[] args = new String[]{
//                "-d", "DUMP",
//                "-ltd", ASSORTED_TBLS_04,
//                "-cfg", HDP2_CDP,
//                "-o", outputDir
//        };
//
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        long check = 0;
////        long check = MessageCode.STORAGE_MIGRATION_REQUIRED_NAMESPACE.getLong();
//
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

//    @Test
//    public void phaseTest() {
//        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.CALCULATED_SQL);
//    }
//
//    @Test
//    public void issueTest() {
//        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
//                Environment.LEFT, 17);
//    }

}
