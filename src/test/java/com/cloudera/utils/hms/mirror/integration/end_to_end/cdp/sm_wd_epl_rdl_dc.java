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

import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.PhaseState;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = com.cloudera.utils.hms.Mirror.class,
        args = {
                "--hms-mirror.config.data-strategy=STORAGE_MIGRATION",
                "--hms-mirror.config.warehouse-directory=/finance/managed-fso",
                "--hms-mirror.config.external-warehouse-directory=/finance/external-fso",
                "--hms-mirror.config.evaluate-partition-location=true",
                "--hms-mirror.config.distcp=PULL",
                "--hms-mirror.config-filename=/config/default.yaml.cdp-cdp",
                "--hms-mirror.config.reset-to-default-location=true",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts.yaml",
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_wd_epl_rdl_dc"
        }
)
@Slf4j
/*
STORAGE_MIGRATION test used to show how to move data from one directory to another, within the same namespace.

Since the -smn is not specified, the namespace is assumed to be the same as the original table location.
The -wd and -ewd are used to define the warehouse directories.  The -epl is used to evaluate the partition locations and
with -rdl, the default location is reset to the new warehouse directory.

There should be no issue now that the default location is reset to the new warehouse directory.

 */
public class sm_wd_epl_rdl_dc extends E2EBaseTest {

    //        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
//                "-wd", "/finance/managed-fso",
//                "-ewd", "/finance/external-fso",
//                "-epl",
//                "-rdl",
//                "-dc",
//                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
//                "-o", outputDir
//        };
//        long rtn = 0;
//        MirrorLegacy mirror = new MirrorLegacy();
//        rtn = mirror.go(args);
//        assertEquals("Return Code Failure: " + rtn, 0, rtn);
//
//        // Read the output and verify the results.
//        DBMirror[] resultsMirrors = getResults(outputDir, EXT_PURGE_ODD_PARTS_03);
//
//        validatePhase(resultsMirrors[0], "web_sales", PhaseState.SUCCESS);
//
//        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "web_sales",  "Alter Table Location",
//                "ALTER TABLE web_sales SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales\"")) {
//            fail("Alter Table Location not found");
//        }
//        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "web_sales",
//                "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location",
//                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"")) {
//            fail("Alter Table Partition Location not found");
//        }
//        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "web_sales",
//                "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location",
//                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451188\"")) {
//            fail("Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location");
//        }

    @Test
    public void issueCountTest() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales", Environment.LEFT, 0);
    }

    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.SUCCESS);
    }

    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        assertEquals("Return Code Failure: " + rtn, 0L, rtn);
    }

    @Test
    public void sqlTest() {
        if (!validateSqlPair("ext_purge_odd_parts", Environment.LEFT, "web_sales", "Alter Table Location",
                "ALTER TABLE web_sales SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales\"")) {
            fail("Alter Table Location not found");
        }
        if (!validateSqlPair("ext_purge_odd_parts", Environment.LEFT, "web_sales",
                "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"")) {
            fail("Alter Table Partition Location not found");
        }
        if (!validateSqlPair("ext_purge_odd_parts", Environment.LEFT, "web_sales",
                "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451188\"")) {
            fail("Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location");
        }
    }

}
