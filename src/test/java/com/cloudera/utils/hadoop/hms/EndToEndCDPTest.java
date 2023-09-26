/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.DBMirror;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import com.cloudera.utils.hadoop.hms.mirror.PhaseState;
import org.junit.Test;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.*;
import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM;
import static org.junit.Assert.*;

public class EndToEndCDPTest extends EndToEndBase {

    @Test
    public void sm_smn_wd_epl_dc() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
//                "-rdl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

        assertEquals("Phase State doesn't match", PhaseState.SUCCESS, resultsMirror.getTableMirrors().get("web_sales").getPhaseState());

        // Verify the results
        // Find ALTER TABLE STATEMENT and verify the location
        Boolean foundAT = Boolean.FALSE;
        Boolean foundOddPart = Boolean.FALSE;
        Boolean foundOddPart2 = Boolean.FALSE;

        for (Pair pair : resultsMirror.getTableMirrors().get("web_sales").getEnvironmentTable(Environment.LEFT).getSql()) {
            if (pair.getDescription().trim().equals("Alter Table Location")) {
                assertEquals("Location doesn't match", "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales\"", pair.getAction());
                foundAT = Boolean.TRUE;
            }
            if (pair.getDescription().trim().equals("Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location")) {
                assertEquals("Location doesn't match", "ALTER TABLE web_sales PARTITION " +
                        "(`ws_sold_date_sk`='2451180') SET LOCATION \"ofs://OHOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"", pair.getAction());
                foundOddPart = Boolean.TRUE;
            }
            if (pair.getDescription().trim().equals("Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location")) {
                assertEquals("Location doesn't match", "ALTER TABLE web_sales PARTITION " +
                        "(`ws_sold_date_sk`='2451188') SET LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\"", pair.getAction());
                foundOddPart2 = Boolean.TRUE;
            }
        }
        assertEquals("Alter Table Location not found", Boolean.TRUE, foundAT);
        assertEquals("Alter Odd Part Location not found", Boolean.TRUE, foundOddPart);
        assertEquals("Alter Odd Part 2 Location not found", Boolean.TRUE, foundOddPart2);
//        System.out.println("Results: " + resultsMirror.toString());
    }

    @Test
    public void sm_wd_epl_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-rdl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

        validatePhase(resultsMirror, "web_sales", PhaseState.SUCCESS);

        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Alter Table Location",
                "ALTER TABLE web_sales SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales\"")) {
            fail("Alter Table Location not found");
        }
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",
                "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"")) {
            fail("Alter Table Partition Location not found");
        }
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",
                "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET LOCATION \"hdfs://HDP50/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451188\"")) {
            fail("Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location");
        }

    }

    @Test
    public void sm_wd_epl_dc() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
//                "-smn", "ofs://OHOME90",
                "-epl",
//                "-rdl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sm_smn_wd_epl_dc_mismatch() {
        /*
        This test uses a configuration (LEFT hcfs namespace) that doesn't match the table/partition prefixes. This should
        fail with warnings.

        FIXED:1. when namespace in table doesn't match the namespace specified in the hcfsNamespace, nothing is translated.
            - This should result in an error and warnings about why this didn't work.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);

        // Read the output and verify the results.
        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

        validatePhase(resultsMirror, "web_sales", PhaseState.ERROR);

    }

    @Test
    public void sm_smn_wd_epl_glm_dc() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
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
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

        validatePhase(resultsMirror, "web_sales", PhaseState.SUCCESS);
        validateTableIssueCount(resultsMirror, "web_sales", Environment.LEFT, 3);
        /*
        - description: "Alter Table Location"
          action: "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales\""
        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location "
          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET\
            \ LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\""
        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location "
          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET\
            \ LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\""
        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2452035' Location "
          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2452035') SET\
            \ LOCATION \"ofs://OHOME90/finance/external-fso/load_web_sales/odd\""
         */
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Alter Table Location",
                "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales\"")) {
            fail("Alter Table Location not found");
        }
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"")) {
            fail("Alter Table Partition Location not found");
        }
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\"")) {
            fail("Alter Table Partition Location not found");
        }


    }

    @Test
    public void sm_wd_epl_glm_dc() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-glm", "/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);

        // Read the output and verify the results.
        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

        validatePhase(resultsMirror, "web_sales", PhaseState.ERROR);
        validateTableIssueCount(resultsMirror, "web_sales", Environment.LEFT, 3);
        // One of the locations is not accounted for in the glm and isn't standard.  So we can't translate it..

    }

    @Test
    public void sm_smn_wd_epl_glm_fel_dc() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
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
                "-fel",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

        validatePhase(resultsMirror, "web_sales", PhaseState.SUCCESS);
        validateTableIssueCount(resultsMirror, "web_sales", Environment.LEFT, 3);
        /*
        - description: "Alter Table Location"
          action: "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales\""
        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location "
          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET\
            \ LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\""
        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location "
          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET\
            \ LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\""
        - description: "Alter Table Partition Spec `ws_sold_date_sk`='2452035' Location "
          action: "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2452035') SET\
            \ LOCATION \"ofs://OHOME90/finance/external-fso/load_web_sales/odd\""
         */
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Alter Table Location",
                "ALTER TABLE web_sales SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales\"")) {
            fail("Alter Table Location not found");
        }
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Alter Table Partition Spec `ws_sold_date_sk`='2451180' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451180') SET LOCATION \"ofs://OHOME90/finance/external-fso/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451180\"")) {
            fail("Alter Table Partition Location not found");
        }
        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Alter Table Partition Spec `ws_sold_date_sk`='2451188' Location",
                "ALTER TABLE web_sales PARTITION (`ws_sold_date_sk`='2451188') SET LOCATION \"ofs://OHOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188\"")) {
            fail("Alter Table Partition Location not found");
        }

    }

    @Test
    public void sm_smn_wd_epl_glm_fel() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
//                "-epl",
                "-glm", "/user/dstreev/datasets/alt-locations/load_web_sales=/finance/external-fso/load_web_sales,/warehouse/tablespace/external/hive=/finance/external-fso",
                "-fel",
//                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

        validatePhase(resultsMirror, "web_sales", PhaseState.SUCCESS);
        validateTableIssueCount(resultsMirror, "web_sales", Environment.RIGHT, 1);

        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Remove table property",
                "ALTER TABLE web_sales UNSET TBLPROPERTIES (\"TRANSLATED_TO_EXTERNAL\")")) {
            fail("Remove Table Property not found");
        }

    }

    @Test
    public void sql_da_ip() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-da", "-ip", "-mao", "-rid",
                "-ltd", ACID_W_PARTS_05, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
//        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

//        validatePhase(resultsMirror, "web_sales", PhaseState.SUCCESS);
//        validateTableIssueCount(resultsMirror, "web_sales", Environment.RIGHT, 1);

//        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Remove table property",
//                "ALTER TABLE web_sales UNSET TBLPROPERTIES (\"TRANSLATED_TO_EXTERNAL\")")) {
//            fail("Remove Table Property not found");
//        }

    }

    @Test
    public void sql_leg_mngd_da_ip() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-da", "-ip", "-mao", "-rid",
                "-ltd", LEGACY_MNGD_NO_PARTS_02, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
//        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

//        validatePhase(resultsMirror, "web_sales", PhaseState.SUCCESS);
//        validateTableIssueCount(resultsMirror, "web_sales", Environment.RIGHT, 1);

//        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Remove table property",
//                "ALTER TABLE web_sales UNSET TBLPROPERTIES (\"TRANSLATED_TO_EXTERNAL\")")) {
//            fail("Remove Table Property not found");
//        }

    }

    @Test
    public void sql_leg_mngd_ewd_da_ip() {
        /*
        Issues: Need to post warning when table/partition(s) new location isn't in the -[e]wd location.

         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-da", "-ip", "-mao", "-ewd", "/my_base_loc/external",
                "-ltd", LEGACY_MNGD_NO_PARTS_02, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
//        DBMirror resultsMirror = getResults(outputDir + "/" + "ext_purge_odd_parts_hms-mirror.yaml");

//        validatePhase(resultsMirror, "web_sales", PhaseState.SUCCESS);
//        validateTableIssueCount(resultsMirror, "web_sales", Environment.RIGHT, 1);

//        if (!validateSqlPair(resultsMirror, Environment.LEFT, "web_sales",  "Remove table property",
//                "ALTER TABLE web_sales UNSET TBLPROPERTIES (\"TRANSLATED_TO_EXTERNAL\")")) {
//            fail("Remove Table Property not found");
//        }

    }

}
