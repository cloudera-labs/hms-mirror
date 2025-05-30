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

package com.cloudera.utils.hms.mirror.config;

import com.cloudera.utils.hms.mirror.MessageCode;
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
                "--hms-mirror.config.warehouse-plans=tpcds_bin_partitioned_orc_10=/warehouse/external:/warehouse/managed",
//                "--hms-mirror.config.warehouse-directory=/warehouse/managed",
//                "--hms-mirror.config.external-warehouse-directory=/warehouse/external",
//                "--hms-mirror.config.sort-dynamic-partition-inserts=true",
//                "--hms-mirror.config.downgrade-acid=true",
//                "--hms-mirror.config.read-only=true",
//                "--hms-mirror.config.sync=true",
//                "--hms-mirror.config.evaluate-partition-location=true",
//                "--hms-mirror.config.intermediate-storage=s3a://my_is_bucket",
//                "--hms-mirror.config.target-namespace=s3a://my_cs_bucket",
                "--hms-mirror.config.align-locations=true",
//                "--hms-mirror.config.distcp=true",
                "--hms-mirror.conversion.test-filename=/test_data/legacy_mngd_parts_01.yaml",
                "--hms-mirror.config.filename=/config/default.yaml.cdh-cdp",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/config/so_wd_epl_rdl"
        })

@Slf4j
public class Test_so_wd_epl_rdl extends E2EBaseTest {
    //        String[] args = new String[]{"-d", "SCHEMA_ONLY",
//                "-epl",
//                "-wd", "/warehouse/tablespace/managed/hive",
//                "-ewd", "/warehouse/tablespace/external/hive",
//                "-rdl",
//                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        long check = DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL.getLong();

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long actual = getReturnCode();
        // Verify the return code.
        long expected = 0L;
//        getCheckCode(MessageCode.DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL);

        assertEquals(expected, actual, "Return Code Failure: ");


    }

}
