/*
 * Copyright (c) 2022. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hadoop.hms.mirror.MessageCode;
import org.junit.Test;

import java.io.IOException;

import static com.cloudera.utils.hadoop.hms.EnvironmentConstants.*;
import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL;
import static org.junit.Assert.assertEquals;

public class ConfigValidationTest extends EndToEndBase {


    @Test
    public void common_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "COMMON",
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir,};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.RESET_TO_DEFAULT_LOCATION.getLong();
        check = check | MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void ei_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void ei_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "EXPORT_IMPORT",
                "--distcp",
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.DISTCP_VALID_STRATEGY.getLong();
        check = check | MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void hybrid_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "HYBRID",
                "--distcp",
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();
        check = check | MessageCode.DISTCP_VALID_STRATEGY.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void linked_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED",
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.RESET_TO_DEFAULT_LOCATION.getLong();
        check = check | MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sm() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();
        check = check | MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);

    }

    @Test
    public void sm_ma_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "STORAGE_MIGRATION",
                "-ma",
                "-dc",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();
        check = check | MessageCode.STORAGE_MIGRATION_NAMESPACE_LEFT_MISSING_RDL_GLM.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void so_f() throws IOException {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-d", "SCHEMA_ONLY",
                "-f",
                "-ltd", ASSORTED_TBLS_06,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.NON_LEGACY_TO_LEGACY.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void so_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-is", "s3a://my_intermediate_storage",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.DISTCP_REQUIRED_FOR_SCHEMA_ONLY_IS.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void so_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();
        check = check | DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void so_rdl_cd_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-ltd", ASSORTED_TBLS_04,
                "-cs", "s3a://my_common_storage",
                "-rdl",
                "--distcp",
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void so_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "--distcp",
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void so_rdl_dc_legacy_mngd_parts() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-rdl",
                "-dc",
                "-ltd", LEGACY_MNGD_PARTS_01,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);

    }

    @Test
    public void so_wd_epl_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY",
                "-epl",
                "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-rdl",
                "-ltd", LEGACY_MNGD_PARTS_01, "-cfg", CDH_CDP,
                "-o", outputDir
        };
        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL.getLong();
        assertEquals("Return Code Failure: " + rtn, check, rtn);
    }

    @Test
    public void so_wd_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{
                "-rdl",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = DISTCP_REQUIRED_FOR_SCHEMA_ONLY_RDL.getLong();
        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-sql",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.SQL_DISTCP_ONLY_W_DA_ACID.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_ma_cs_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-ma",
                "--distcp",
                "-cs", "s3a://my_common_bucket",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.SQL_DISTCP_ONLY_W_DA_ACID.getLong();
        check = check | MessageCode.SQL_DISTCP_ACID_W_STORAGE_OPTS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_mao_da_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "-da",
                "--distcp",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_STRATEGY.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);

    }

    @Test
    public void sql_mao_dc_ewd() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "-mao",
                "--distcp",
                "-ewd", "/warehouse/external",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.SQL_DISTCP_ONLY_W_DA_ACID.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_STRATEGY.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

    @Test
    public void sql_rdl_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = getOutputDirBase() + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL",
                "--distcp",
                "-rdl",
                "-ltd", ASSORTED_TBLS_04,
                "-cfg", HDP2_CDP,
                "-o", outputDir};

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.RESET_TO_DEFAULT_LOCATION_WITHOUT_WAREHOUSE_DIRS.getLong();
        check = check | MessageCode.SQL_DISTCP_ONLY_W_DA_ACID.getLong();

        assertEquals("Return Code Failure: " + rtn + " doesn't match: " + check, rtn, check);
    }

}