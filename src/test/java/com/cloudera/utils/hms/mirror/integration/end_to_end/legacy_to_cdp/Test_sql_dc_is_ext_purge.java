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
                "--hms-mirror.config.data-strategy=SQL",
//                "--hms-mirror.config.migrate-acid=true",
//                "--hms-mirror.config.migrate-acid-only=true",
                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
//                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.read-only=true",
//                "--hms-mirror.config.sync=true",
//                "--hms-mirror.config.evaluate-partition-location=true",
                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
//                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
//                "--hms-mirror.config.reset-to-default-location=true",
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.hdp2-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/legacy_cdp/sql_dc_is_ext_purge"
        })
@Slf4j
/*
Issue 86 https://github.com/cloudera-labs/hms-mirror/issues/86
 */
public class Test_sql_dc_is_ext_purge extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SQL",
//                "-dc", "-is", "s3a://my_intermediate_bucket",
//                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        long check = 0;
//
//        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);
//
//        DBMirror[] resultsMirrors = getResults(outputDir,EXT_PURGE_ODD_PARTS_03);
//          TODO: distcp Checks.
//        String line = getDistcpLine(outputDir, resultsMirrors, 0, Environment.RIGHT, 1, 0);
//        String checkStr = "s3a://my_intermediate_bucket/hms_mirror_working/"
//                + ConnectionPoolService.getInstance().getConfig().getRunMarker()
//                + "/ext_purge_odd_parts.db";
//        assertEquals("Right Distcp source file isn't correct.", checkStr, line);

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();

        // One of the partitions is non-standard and can't be translated without a specific warehouse plan.  So we
        // can't translate that location.

        // Verify the return code.
        long check = getCheckCode();
        // DISTCP Adjusted automatically.
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }


}
