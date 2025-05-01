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
                "--hms-mirror.config.storage-migration-namespace=ofs://OHOME90",
                "--hms-mirror.config.warehouse-directory=/finance/managed-fso",
                "--hms-mirror.config.external-warehouse-directory=/finance/external-fso",
//                "--hms-mirror.config.evaluate-partition-location=true",
                // TODO: With align on, we fail.  With it off, we pass.  Need to investigate.
//                "--hms-mirror.config.align-locations=true",
                // TODO: Need to check this. I think there is a scenario with
                //   RELATIVE and DISTCP that we AREN'T getting partition locations,
                //      which could yield issues for non-standard locations.  And we need
                //      to warn about that.
                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp_cdp/so_smn_wd_epl_dc"
        })
@Slf4j
public class Test_so_smn_wd_epl_dc extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SCHEMA_ONLY",
//                "-wd", "/finance/managed-fso",
//                "-ewd", "/finance/external-fso",
//                "-smn", "ofs://OHOME90",
//                "-epl",
//                "-dc",
//                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }

}
