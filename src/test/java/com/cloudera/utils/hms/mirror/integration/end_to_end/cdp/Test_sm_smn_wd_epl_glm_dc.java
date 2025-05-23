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

import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.cli.Mirror;
import com.cloudera.utils.hms.mirror.integration.end_to_end.E2EBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = Mirror.class,
        args = {
                "--hms-mirror.config.output-dir=${user.home}/.hms-mirror/test-output/e2e/cdp/sm_smn_wd_epl_glm_dc",
                "--hms-mirror.conversion.test-filename=/test_data/ext_purge_odd_parts_01.yaml",
                "--hms-mirror.config.storage-migration-strict=true",
                "--hms-mirror.config.data-movement-strategy=SQL",
                "--hms-mirror.config.global-location-map=/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso,/user/dstreev/datasets/alt-locations/web_sales=/finance/external-fso/load_web_sales"
        })
@ActiveProfiles("e2e-cdp-sm_smn_wd_epl_dc")
@Slf4j
/*
Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.
 */
public class Test_sm_smn_wd_epl_glm_dc extends E2EBaseTest {
    @Test
    public void phaseTest() {
        validatePhase("ext_purge_odd_parts", "web_sales", PhaseState.CALCULATED_SQL);
    }

    /*
        public void sm_smn_wd_epl_glm_dc() {

    String nameofCurrMethod = new Throwable()
            .getStackTrace()[0]
            .getMethodName();

    String outputDir = getOutputDirBase() + nameofCurrMethod;

    String[] args = new String[]{"-d", "STORAGE_MIGRATION",
            "-wd", "/finance/managed-fso",
            "-ewd", "/finance/external-fso",
            "-smn", "ofs://OHOME90",
            "-epl",
            "-glm", "/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso",
//                "-rdl",
            "-dc",
            "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
            "-o", outputDir
    };
    long rtn = 0;
    MirrorLegacy mirror = new MirrorLegacy();
    rtn = mirror.go(args);
    assertEquals("Return Code Failure: " + rtn, 0, rtn);

    // Read the output and verify the results.
    DBMirror[] resultsMirrors = getResults(outputDir, EXT_PURGE_ODD_PARTS_03);

    validatePhase(resultsMirrors[0], "web_sales", PhaseState.CALCULATED_SQL);
    validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.LEFT, 3);

//        - description: "Alter Table Location"
//          action: "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales\""
//        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location "
//          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET\
//            \ LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\""
//        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location "
//          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET\
//            \ LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\""
//        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2452035' Location "
//          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2452035') SET\
//            \ LOCATION \"ofs://OHOME90/finance/external-fso/load_web_sales/odd\""

        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "web_sales",  "Alter Table Location",
            "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales\"")) {
        fail("Alter Table Location not found");
    }
        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "web_sales",  "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location",
            "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"")) {
        fail("Alter Table Partition Location not found");
    }
        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "web_sales",  "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location",
            "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\"")) {
        fail("Alter Table Partition Location not found");
    }


}
     */
    @Test
    public void returnCodeTest() {
        // Get Runtime Return Code.
        long rtn = getReturnCode();
        // Verify the return code.
        long check = 0L;
        assertEquals(check, rtn, "Return Code Failure: " + rtn);
    }


    @Test
    public void validateTableIssueCount() {
        validateTableIssueCount("ext_purge_odd_parts", "web_sales",
                Environment.LEFT, 2);

//        assertEquals("Issue Count not as expected", 3,
//                getConversion().getDatabase("ext_purge_odd_parts")
//                        .getTableMirrors().get("web_sales")
//                        .getEnvironmentTable(Environment.LEFT).getIssues().size());
    }

    @Test
    public void validateDBExtlLocation() {
        validateDBLocation("ext_purge_odd_parts", Environment.RIGHT,
                "ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db");
    }

    @Test
    public void validateDBMngdlLocation() {
        validateDBManagedLocation("ext_purge_odd_parts", Environment.RIGHT,
                "ofs://OHOME90/finance/managed-fso/ext_purge_odd_parts.db");
    }

}
