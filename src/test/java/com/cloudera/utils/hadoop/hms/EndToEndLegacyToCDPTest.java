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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EndToEndLegacyToCDPTest extends EndToEndBase {

    @Test
    public void common() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "COMMON",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void common_ma() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "COMMON",
                "-sql", "-ma",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.VALID_ACID_STRATEGIES.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void dump() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "DUMP",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = 0;
//        long check = MessageCode.STORAGE_MIGRATION_REQUIRED_NAMESPACE.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void dump_ma_epl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "DUMP",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void ei() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 1;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void ei_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-sql", "-is", INTERMEDIATE_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 1;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void ei_mao() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-mao",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3; // 3 acid from older version
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void hybrid() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void hybrid_ep_minus1() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "HYBRID",
                "-ep", "-1",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void hybrid_ma_epl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void hybrid_mao_da_ro_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "-mao", "-da",
                "-ro",
                "-cs", COMMON_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void linked() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void linked_wd_epl_dc_legacy_mngd_w_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.DISTCP_VALID_STRATEGY.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void linked_wd_epl_legacy_mngd_w_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void linked_wd_epl_w_odd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

        // Testing for existing schemas.
        outputDir = outputDir + "/2";
        args = new String[]{
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        rtn = 0;
        mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_cs_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-cs", "s3a://my_common_bucket",
                "--distcp",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", CDH_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_cs_rdl_wd_dc() {
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_dbo() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-dbo",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

    @Test
    public void so_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_dc_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "--distcp",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", CDH_CDP,
                "-o", outputDir
        };
//        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_dc_legacy_mngd_w_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_epl_dc_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_epl_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_epl_wd_dc_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_is_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-is", "s3a://my_intermediate_bucket",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_is_dc_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-is", "s3a://my_intermediate_bucket",
                "--distcp",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", CDH_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_legacy_mngd_w_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_ma_epl() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_ma_wd_epl_dc_w_parts() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-dc",
                "-ltd", ACID_W_PARTS_05, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);
    }

    @Test
    public void so_ma_wd_epl_w_parts() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", ACID_W_PARTS_05, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void so_ma_wd_rdl_epl_dc_ext_purge_w_odd_parts() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-rdl",
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_mao_4() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-mao", "4",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_mao_wd_rdl_dc() {
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 3; // 3 acid tables. can't dc them.
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

//        long rtn = ;
        Mirror mirror = new Mirror();
        long rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void so_ro_sync() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ro", "-sync", "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 3;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void so_ro_tf() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-tf", "call_center|store_sales",
                "-ro", "-sql",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void so_to() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-to",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0; // acid tables.
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_fel() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
//                "-rdl",
//                "-wd", "/wrehouse/tablespace/managed/hive",
//                "-ewd", "/wrehouse/tablespace/external/hive",
//                "-glm", "/warehouse/external/hive=/chuck/me",
                "-fel",
                "-dc",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,ASSORTED_TBLS_04);

        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.SUCCESS);
        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.SUCCESS);
        validatePhase(resultsMirrors[0], "legacy_mngd_01", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "ext_part_01", Environment.RIGHT, 2);
        validateTableLocation(resultsMirrors[0], "ext_part_01", Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");

    }

    @Test
    public void so_wd_fel() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
//                "-rdl",
                "-wd", "/wrehouse/tablespace/managed/hive", // should trigger warnings, but not affect location.
                "-ewd", "/wrehouse/tablespace/external/hive",
//                "-glm", "/warehouse/external/hive=/chuck/me",
                "-fel",
                "-dc",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,ASSORTED_TBLS_04);

        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "ext_part_01", Environment.RIGHT, 3);
        validateTableLocation(resultsMirrors[0], "ext_part_01", Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/external/hive/assorted_test_db.db/ext_part_01");

    }

    @Test
    public void so_wd_glm_fel() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
//                "-rdl",
                "-wd", "/wrehouse/tablespace/managed/hive",
                "-ewd", "/wrehouse/tablespace/external/hive", // trigger warnings.
                "-glm", "/warehouse/tablespace/external/hive=/chuck/me", // will adjust location
                "-fel",
                "-dc",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,ASSORTED_TBLS_04);

        validatePhase(resultsMirrors[0], "ext_part_01", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "ext_part_01", Environment.RIGHT, 3);
        validateTableLocation(resultsMirrors[0], "ext_part_01", Environment.RIGHT, "hdfs://HOME90/chuck/me/assorted_test_db.db/ext_part_01");

    }

    @Test
    public void so_wd_dc_legacy_mngd_w_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso", // will trigger warnings.
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,LEGACY_MNGD_PARTS_01);

        validatePhase(resultsMirrors[0], "web_sales", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 3);
        validateTableLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales");

    }

    @Test
    public void so_wd_epl_dc_ext_purge_w_odd_parts() {
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
                "-epl",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,EXT_PURGE_ODD_PARTS_03);

        validatePhase(resultsMirrors[0], "web_sales", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 18);
        validatePartitionCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 16);
        validateTableLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales");
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452035", "hdfs://HOME90/user/dstreev/datasets/alt-locations/load_web_sales/odd");
        // ws_sold_date_sk=2451188: "hdfs://HOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188"
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2451188", "hdfs://HOME90/user/dstreev/datasets/alt-locations/web_sales/ws_sold_date_sk=2451188");
        // ws_sold_date_sk=2451793: "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451793"
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2451793", "hdfs://HOME90/warehouse/tablespace/external/hive/ext_purge_odd_parts.db/web_sales/ws_sold_date_sk=2451793");

    }

    @Test
    public void so_wd_epl_dc_legacy_mngd_w_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,LEGACY_MNGD_PARTS_01);

        validatePhase(resultsMirrors[0], "web_sales", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1827);
        validatePartitionCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1824);
        validateTableLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales");
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452033", "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452033");
        // ws_sold_date_sk=2452036: "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452036"
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452036", "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452036");

    }

    @Test
    public void so_wd_epl_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-rdl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 1, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,LEGACY_MNGD_PARTS_01);

        // Will error because there isn't a mapping required for 'dc' to align locations.
        validatePhase(resultsMirrors[0], "web_sales", PhaseState.ERROR);

        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 3651);
        validatePartitionCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1824);

    }

    @Test
    public void so_wd_epl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
//                "-rdl",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,LEGACY_MNGD_PARTS_01);

        // Will error because there isn't a mapping required for 'dc' to align locations.
        validatePhase(resultsMirrors[0], "web_sales", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1827);
        validatePartitionCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1824);
        validateTableLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales");
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452033", "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452033");
        // ws_sold_date_sk=2452036: "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452036"
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452036", "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452036");

    }

    @Test
    public void so_wd_epl_rdl_glm_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-rdl",
                "-dc",
                "-glm", "/apps/hive/warehouse=/warehouse/tablespace/external/hive",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);

        // Read the output and verify the results.
        DBMirror[] resultsMirrors = getResults(outputDir,LEGACY_MNGD_PARTS_01);

        // Will error because there isn't a mapping required for 'dc' to align locations.
        validatePhase(resultsMirrors[0], "web_sales", PhaseState.SUCCESS);

        validateTableIssueCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1827);
        validatePartitionCount(resultsMirrors[0], "web_sales", Environment.RIGHT, 1824);
        validateTableLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/external/hive/tpcds_bin_partitioned_orc_10.db/web_sales");
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452033", "hdfs://HOME90/warehouse/tablespace/external/hive/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452033");
        // ws_sold_date_sk=2452036: "hdfs://HOME90/apps/hive/warehouse/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452036"
        validatePartitionLocation(resultsMirrors[0], "web_sales", Environment.RIGHT, "ws_sold_date_sk=2452036", "hdfs://HOME90/warehouse/tablespace/external/hive/tpcds_bin_partitioned_orc_10.db/web_sales/ws_sold_date_sk=2452036");

        validateDBLocation(resultsMirrors[0], Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/external/hive/tpcds_bin_partitioned_orc_10.db");
        validateDBManagedLocation(resultsMirrors[0], Environment.RIGHT, "hdfs://HOME90/warehouse/tablespace/managed/hive/tpcds_bin_partitioned_orc_10.db");
    }

    @Test
    public void so_wd_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-sql",
                "-rdl",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);
    }

    @Test
    public void so_wd_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-rdl",
                "--distcp",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_wd_rdl_dc_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-rdl",
                "-dc",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void so_wd_tf_epl_dc_legacy_mngd_w_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-tf", "web_sales",
                "-epl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void sql_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-cs", "s3a://my_common_bucket",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_ext_purge() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = 0;

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sql_dc_ext_purge() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-dc",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = 0;

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    /*
    Issue 86 https://github.com/cloudera-labs/hms-mirror/issues/86
     */
    @Test
    public void sql_dc_is_ext_purge() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-dc", "-is", "s3a://my_intermediate_bucket",
                "-ltd", EXT_PURGE_ODD_PARTS_03, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = 0;

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

        DBMirror[] resultsMirrors = getResults(outputDir,EXT_PURGE_ODD_PARTS_03);

        String line = getDistcpLine(outputDir, resultsMirrors, 0, Environment.RIGHT, 1, 0);
        assertEquals("Right Distcp source file isn't correct.", "s3a://my_intermediate_bucket/hms_mirror_working/20230927_084820/ext_purge_odd_parts.db", line);

    }

    /*
    TODO: Bug...  For EXTERNAL tables the 'transfer' table isn't 'managed' (in legacy) so
            when it's deleted, the data isn't cleaned up.
            Currently, the intermediate storage location is unique and can be
              cleaned up after the fact.
     */
    @Test
    public void sql_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-is", "s3a://my_intermediate_bucket",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_is_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-sql",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_ma_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "-is", INTERMEDIATE_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    /*
    TODO: We can implement this.  But for now, don't allow.
     */
    public void sql_ma_is_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma", "-dc",
                "-is", INTERMEDIATE_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.SQL_DISTCP_ONLY_W_DA_ACID.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check * -1, rtn);
    }

    // ====
    @Test
    public void sql_ma_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-cs", COMMON_STORAGE,
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_ma_epl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "-epl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
    }

    @Test
    public void sql_ma_wd_epl_w_parts() {
        // Issue:
        /*
        FIXED: 1. Partitions are on HOME90, not ofs..
         */
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "-wd", "/finance/managed-fso",
                "-ewd", "/finance/external-fso",
                "-epl",
                "-ltd", ACID_W_PARTS_05, "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertEquals("Return Code Failure: " + rtn, 0, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir};


        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_mao_ewd_da_dc() {
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_mao_ewd_da_dc_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-ewd", "/warehouse/external",
                "-da",
                "--distcp",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_mao_sdpi() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-sdpi",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
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
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);
    }

    @Test
    public void sql_ro() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-sql", "-ro",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 3;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        // Should fail because DB dir doesn't exist.  RO assumes data moved already.
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check * -1, check * -1, rtn);

    }

    @Test
    public void sql_sync() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "SQL", "-sync",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = 0;

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, check, rtn);

    }

}
