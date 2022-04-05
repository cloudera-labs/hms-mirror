/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.MessageCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertTrue;

public class MirrorTest extends MirrorTestBase {

//    private final String HDP2_CDP = "default.yaml.hdp2-cdp";
//    private final String CDP_CDP = "default.yaml.cdp-cdp";
//    //    private Boolean execute = Boolean.FALSE;
//    private String[] execArgs = {"-e", "--accept"};
//    private Boolean execute = Boolean.FALSE;
//    private String homedir = null;
//    private String working_db = "tpcds_bin_partitioned_orc_10";
//    private String intermediate_storage = "s3a://my_is_bucket";
//    private String common_storage = "s3a:/my_cs_bucket";
//
//    protected void reset() {
//        DateFormat df = new SimpleDateFormat("yyyymmDDHHMMSS");
//        String outputDir = String.format(homedir + System.getProperty("file.separator") + "hms-mirror-reports/reset_right/" + df.format(new Date()));
//        String[] args = new String[]{"-db", working_db, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP, "-rr"};
//        args = toExecute(args, execArgs);
//
//        long rtn = 0;
//        Mirror mirror = new Mirror();
//        rtn = mirror.go(args);
//        assertTrue("Return Code Failure", rtn == 0);
//    }
//
//    private String[] toExecute(String[] one, String[] two) {
//        String[] rtn = one;
//        if (execute) {
//            rtn = new String[one.length + two.length];
//            System.arraycopy(one, 0, rtn, 0, one.length);
//            System.arraycopy(two, 0, rtn, one.length, two.length);
//        }
//        return rtn;
//    }
//
//    @Before
//    public void setUp() throws Exception {
//        homedir = System.getProperty("user.home");
//
//        // Override default db for test run.
//        String lclWorkingDb = System.getenv("DB");
//        if (lclWorkingDb != null) {
//            working_db = lclWorkingDb;
//        }
//
//        // Set the execute flags for test suite.
//        String execStr = System.getenv("EXECUTE");
//        if (execStr != null) {
//            execute = Boolean.valueOf(execStr);
//        }
//
//        // Override intermediate_storage for test run.
//        String lclIntermediate_storage = System.getenv("IS");
//        if (lclIntermediate_storage != null) {
//            intermediate_storage = lclIntermediate_storage;
//        }
//
//        // Override intermediate_storage for test run.
//        String lclcommon_storage = System.getenv("CS");
//        if (lclcommon_storage != null) {
//            common_storage = lclcommon_storage;
//        }
//
//    }
//
    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void test_acid_b_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_b_leg";
        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", working_db, "-mao", "4", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_exp_imp() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_exp_imp";
        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_exp_imp_da() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_exp_imp_da";
        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", working_db, "-mao", "-da", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_exp_imp_da_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_exp_imp_da_leg";
        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", working_db, "-mao", "-da", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_exp_imp_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_exp_imp_leg";
        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_hybrid_da_cs_all_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_cs_all_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-ma", "-da", "-cs", "s3a://my_common_storage", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_hybrid_da_cs_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_cs_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-da", "-cs", common_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_hybrid_da_cs_r_all_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_r_all_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-ma", "-da", "-r", "-cs", common_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_acid_hybrid_da_cs_r_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_r_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-da", "-r", "-cs", common_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_acid_hybrid_da_cs_ro_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_cs_ro_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-da", "-ro", "-cs", common_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_hybrid_da_is_all_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_is_all_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-ma", "-da", "-is", intermediate_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_hybrid_da_is_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_is_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-da", "-is", intermediate_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_hybrid_da_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-da", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

    }

    @Test
    public void test_acid_hybrid_da_r_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_da_r_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-da", "-r", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_acid_hybrid_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_hybrid_r_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_hybrid_r_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-mao", "-r", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.REPLACE_ONLY_WITH_SQL.getLong();
        check = check | MessageCode.REPLACE_ONLY_WITH_DA.getLong();
        assertTrue("Return Code Failure", rtn == check);

    }

    @Test
    public void test_acid_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_leg";
        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_all() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_all";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-ma", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_cs() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_cs";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-cs", common_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_da";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-da", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_cs() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_da_cs";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-cs", common_storage, "-da", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_cs_r_all_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_da_cs_r_all_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-ma", "-da", "-r", "-cs", common_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_da_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-da", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_leg_cs() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_da_leg_cs";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-cs", common_storage, "-da", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_r_all_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_da_r_all_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-ma", "-da", "-r", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_da_r_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_da_r_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-da", "-r", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    // ====
    @Test
    public void test_acid_sql_leg_cs() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_leg_cs";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-cs", common_storage, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_acid_sql_r_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/acid_sql_r_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "-r", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.REPLACE_ONLY_WITH_DA.getLong();
        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_common_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/common_leg";
        String[] args = new String[]{"-d", "COMMON", "-db", working_db, "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    // ====

    @Test
    public void test_dbo() {

        reset();

        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/dbo";
        String[] args = new String[]{"-db", working_db, "-dbo", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

        // We can do deeper testing on results here.
        // Conversion conversion = mirror.getConversion();

    }

    @Test
    public void test_dump_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/dump_leg";
        String[] args = new String[]{"-d", "DUMP", "-db", working_db, "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_exp_imp_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/exp_imp_leg";
        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", working_db, "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_hybrid_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/hybrid_leg";
        String[] args = new String[]{"-d", "HYBRID", "-db", working_db, "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_linked_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/linked_leg";
        String[] args = new String[]{"-d", "LINKED", "-db", working_db, "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_reset_right() {
        reset();
    }

    @Test
    public void test_so_leg() {
        reset();

        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_leg/1";
        String[] args = new String[]{"-db", working_db, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

        outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_leg/2";
        args = new String[]{"-db", working_db, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        rtn = 0;
        mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

    }

    @Test
    public void test_so_ro_leg() {
        reset();

        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_ro_leg";
        String[] args = new String[]{"-db", working_db, "-ro", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_so_ro_sync_leg() {

        reset();

        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_ro_sync_leg";
        String[] args = new String[]{"-db", working_db, "-ro", "-sync", "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_so_ro_tf_leg() {
        reset();

        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_ro_tf_leg";
        String[] args = new String[]{"-db", working_db, "-tf", "call_center|store_sales", "-ro", "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_sp_limit() {
        reset();

        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/sp_limit";
        String[] args = new String[]{"-db", working_db, "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP, "-sp", "-1"};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);

        // We can do deeper testing on results here.
        // Conversion conversion = mirror.getConversion();

    }

    @Test
    public void test_sql_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/sql_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-sql", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure", rtn == 0);
    }

    @Test
    public void test_sql_ro_leg() {
        reset();
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/sql_ro_leg";
        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-sql", "-ro", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        // Should fail because DB dir doesn't exist.  RO assumes data moved already.
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

    @Test
    public void test_sql_sync_leg() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/sql_sync_leg";
        String[] args = new String[]{"-db", working_db, "-d", "SQL", "-sync", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.VALID_SYNC_STRATEGIES.getLong();

        assertTrue("Return Code Failure", rtn == check);
    }

}