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
import com.cloudera.utils.hadoop.hms.mirror.MessageCode;
import com.cloudera.utils.hadoop.hms.mirror.PhaseState;
import org.junit.Test;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.*;
import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.SQL_DISTCP_ONLY_W_DA_ACID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EndToEndCDPtoCDPTest extends EndToEndBase {

    @Test
    public void common_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "COMMON",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void dump_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "DUMP",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void ei_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-sql", "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 1;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void ei_bad_hcfsns_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-sql", "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP_BNS,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void ei_is_ep() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-sql", "-is", INTERMEDIATE_STORAGE,
                "-ep", "500",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void ei_ma_da_sync() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-ma",
//                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-da",
                "-sync",
                "-ltd", EXISTS_07,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void ei_ma_exists() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-ma",
//                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
//                "-sync",
                "-ltd", EXISTS_07,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 2; // errors on 2 table because they exist already.
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void ei_ma_sync() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-ma",
//                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-sync",
                "-ltd", EXISTS_07,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void ei_ma_wd_ep() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-ma", "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ep", "500",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void ei_mao_da() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-mao", "-da",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 1; // partition counts exceed limit of 100 (default).
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void ei_mao_da_ip() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-mao", "-da", "-ip",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.VALID_ACID_DA_IP_STRATEGIES.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void ei_mao_da_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-mao", "-da", "-is", INTERMEDIATE_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 1;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void ei_mao_ep() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-mao",
                "-ep", "500",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        // theres 1 non acid table in the test dataset.
        // BUG in loadTestData..  Doesn't check -mao option after testdata loaded.

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void ei_rdl_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-sql",
                "-rdl",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 1; // One table exceed partition count limit of 100 (default).
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void hybrid_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0; // exceed partition count
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void hybrid_bad_hcfsns_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP_BNS,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3; // exceed partition count and bad hcfsns
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void hybrid_ma_da_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma", "-da", "-cs", COMMON_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hybrid_ma_da_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma", "-da", "-is", INTERMEDIATE_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hybrid_ma_wd_epl() {
        // Issues:
        /*
        FIXED: 1. Warehouse Locations not being set.
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma",
                "-wd", "/finance/mngd-hive",
                "-ewd", "/finance/ext-hive",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void hybrid_mao() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-mao",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hybrid_mao_da() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-mao", "-da",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hybrid_mao_da_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-mao", "-da", "-cs", COMMON_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hybrid_mao_da_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-mao", "-da", "-is", INTERMEDIATE_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hybrid_mao_da_ro_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-mao", "-da", "-ro", "-cs", COMMON_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hybrid_mao_da_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-mao", "-da",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void hydrid_ma_epl() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void sm_ma_wd_dc_at_fel() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "STORAGE_MIGRATION",
//                "-smn", COMMON_STORAGE,
                "-ma",
                "-dc",
                "-fel",
                "-wd", "/warehouseEC/managed/hive", "-ewd", "/warehouse/external",
                "-glm", "/warehouse/tablespace/managed=/warehouseEC/managed",
                "-ltd", ACID_W_PARTS_05,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sm_smn_ma_4_rdl_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "STORAGE_MIGRATION",
                "-smn", COMMON_STORAGE,
                "-ma", "4",
                "-rdl",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sm_smn_ma_4_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "STORAGE_MIGRATION",
                "-smn", COMMON_STORAGE,
                "-ma", "4",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sm_smn_ma_6_da_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "STORAGE_MIGRATION",
                "-smn", COMMON_STORAGE,
                "-ma", "6", "-da",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sm_smn_ma_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "STORAGE_MIGRATION",
                "-smn", COMMON_STORAGE,
                "-ma",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);


    }

    @Test
    public void sm_smn_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "STORAGE_MIGRATION",
                "-smn", COMMON_STORAGE,
                "-wd", "/warehouse/managed_tables", "-ewd", "/warehouse/external_tables",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sm_smn_wd_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "STORAGE_MIGRATION",
                "-smn", COMMON_STORAGE,
                "-wd", "/warehouse/managed_tables",
                "-ewd", "/warehouse/external_tables",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0; // because 3 of the tables are acid.
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);


        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,ASSORTED_TBLS_04);

        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.SUCCESS);
        validateTableIssueCount(resultsMirrors[0], "ext_part_01", Environment.LEFT, 441);

        if (!validateSqlPair(resultsMirrors[0], Environment.LEFT, "ext_part_01", "Alter Table Location",
                "ALTER TABLE ext_part_01 SET LOCATION \"s3a://my_cs_bucket/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01\"")) {
            fail("Alter Table Location not found");
        }

    }

    @Test
    public void so_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);


    }

    @Test
    public void so_cine_sync_epl_tf_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "SCHEMA_ONLY"
                , "-cine"
                , "-epl"
                , "-sync"
                , "-tf", "web_sales"
                , "-ltd", EXISTS_PARTS_08
                , "-cfg", CDP_CDP
                , "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_cs_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-cs", "s3a://my_common_storage",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_cs_rdl_dc_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-cs", "s3a://my_common_storage",
                "-rdl",
                "--distcp",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_exist_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ltd", EXISTS_07,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 1; // because tables exist already.
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_ma_exist_sync_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ma",
                "-sync",
                "-ltd", EXISTS_07,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_mao() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-mao",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void so_mao_rdl_wd_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-mao",
                "-rdl", "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3; //acid tables.
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_rdl_dc_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-rdl",
                "-dc",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_ro() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ro",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_smn_wd_epl() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_smn_wd_epl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-smn", "ofs://OHOME90",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDP_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void sql_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_bad_hcfsns_02() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP_BNS,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3; // wrong ns
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_cine_sync_epl_tf_dc_01() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "SQL"
                , "-dc"
                , "-cine"
                , "-epl"
                , "-sync"
                , "-tf", "web_sales"
                , "-ltd", EXISTS_PARTS_08
                , "-cfg", CDP_CDP
                , "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void sql_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-sql", "-is", "s3a://my_is_bucket/somewhere/here",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_ma() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_ma_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-dc",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = SQL_DISTCP_ONLY_W_DA_ACID.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, rtn, check * -1);
    }

    @Test
    public void sql_mao_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao", "-cs", COMMON_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_cs_da() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-cs", COMMON_STORAGE,
                "-da",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_cs_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao", "-cs", COMMON_STORAGE,
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_da() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao", "-da",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_da_dc_ewd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-da",
                "--distcp",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_da_ip() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao", "-da", "-ip",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_da_rdl_dc_wd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "SQL",
                "-mao",
                "-da",
                "-rdl",
                "--distcp",
                "-ewd", "/warehouse/external",
                "-wd", "/warehouse/managed",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao", "-is", INTERMEDIATE_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_wd_da_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-ewd", "/warehouse/external",
                "-wd", "/warehouse/managed",
                "-da",
                "-rdl",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_wd_da_rdl_is_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-ewd", "/warehouse/external",
                "-wd", "/warehouse/managed",
                "-da",
                "-rdl",
                "-is", "s3a://my_is_bucket",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", CDP_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

}
