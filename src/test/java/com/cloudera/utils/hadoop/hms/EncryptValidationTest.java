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
import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static com.cloudera.utils.hadoop.hms.TestSQL.TBL_INSERT;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class EncryptValidationTest extends MirrorTestBase {

    private static final String PKEY = "test";
//    private String PW = ""

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DataState.getInstance().setConfiguration(CDP_ENCRYPT);
        if (DataState.getInstance().getPopulate() == null) {
            DataState.getInstance().setPopulate(Boolean.FALSE);
        }
        dataSetup01();
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(DATACLEANUP.LEFT);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        dataCleanup(DATACLEANUP.LEFT);
    }

//    public Boolean dataSetup01() {
//        if (!DataState.getInstance().isDataCreated("set01")) {
//            String nameofCurrMethod = new Throwable()
//                    .getStackTrace()[0]
//                    .getMethodName();
//
//            String outputDir = outputDirBase + nameofCurrMethod;
//
//            String[] args = new String[]{"-d", "STORAGE_MIGRATION", "-smn", "s3a://something_not_relevant",
//                    "-pkey", PKEY,
//                    "-wd", "/hello", "-ewd", "/hello-ext",
//                    "-db", DataState.getInstance().getWorking_db(), "-o", outputDir,
//                    "-cfg", DataState.getInstance().getConfiguration()};
//            args = toExecute(args, execArgs, Boolean.TRUE);
//
//            List<Pair> leftSql = new ArrayList<Pair>();
//            build_use_db(leftSql);
//
//            List<String[]> dataset = null;
//            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
//                dataset = getDataset(2, 200, null);
//            }
//            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, TBL_INSERT, dataset, leftSql, new String[]{"acid_01", "2"});
//            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
//                dataset = getDataset(2, 400, null);
//            }
//            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, TBL_INSERT, dataset, leftSql, new String[]{"acid_02", "6"});
//            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
//                dataset = getDataset(3, 400, null);
//            }
//            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS_PARTITIONED, TBL_INSERT_PARTITIONED, dataset, leftSql, new String[]{"acid_03", "6"});
//            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
//                dataset = getDataset(2, 2000, 500);
//            }
//            build_n_populate(CREATE_EXTERNAL_TBL_PARTITIONED, TBL_INSERT_PARTITIONED, dataset, leftSql, new String[]{"ext_part_01"});
//            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
//                dataset = getDataset(2, 2000, null);
//            }
//            build_n_populate(CREATE_EXTERNAL_TBL, TBL_INSERT, dataset, leftSql, new String[]{"ext_part_02"});
//
//            Mirror cfgMirror = new Mirror();
//            long rtn = cfgMirror.setupSql(args, leftSql, null);
//            DataState.getInstance().setDataCreated("set01", Boolean.TRUE);
//        }
//        return Boolean.TRUE;
//    }

    protected static Boolean dataCleanup(DATACLEANUP datacleanup) {
        if (DataState.getInstance().isCleanUp()) {
            String nameofCurrMethod = new Throwable()
                    .getStackTrace()[0]
                    .getMethodName();

            String outputDir = homedir + separator + "hms-mirror-reports" + separator + MirrorTestBase.class.getSimpleName() +
                    separator + nameofCurrMethod;

            String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                    "-d", "DUMP",
                    "-pkey", PKEY,
                    "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
            args = toExecute(args, execArgs, Boolean.TRUE);

            List<Pair> leftSql = new ArrayList<Pair>();
            String dropDb = MessageFormat.format(MirrorConf.DROP_DB, DataState.getInstance().getWorking_db());
            Pair l01p = new Pair("DROP DB: " + DataState.getInstance().getWorking_db(), dropDb);
            leftSql.add(l01p);

            List<Pair> rightSql = new ArrayList<Pair>();
            String dropDb2 = MessageFormat.format(MirrorConf.DROP_DB, DataState.getInstance().getWorking_db());
            Pair r01p = new Pair("DROP DB: " + DataState.getInstance().getWorking_db(), dropDb2);
            rightSql.add(r01p);

            Mirror cfgMirror = new Mirror();

            long rtn = 0l;
            switch (datacleanup) {
                case LEFT:
                    rtn = cfgMirror.setupSqlLeft(args, leftSql);
                    break;
                case RIGHT:
                    rtn = cfgMirror.setupSqlRight(args, rightSql);
                    break;
                case BOTH:
                    rtn = cfgMirror.setupSql(args, leftSql, rightSql);
                    break;
            }
        }
        return Boolean.TRUE;
    }

    @Test
    public void test_encrypt_test() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-pkey", PKEY,
                "-p", "myspecialpassword"};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.PASSWORD_CFG.getLong();

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_decrypt_test() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-pkey", PKEY,
                "-dp", "FNLmFEI0F/n8acz45c3jVExMounSBklX"};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.PASSWORD_CFG.getLong();

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_p_test() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{
                "-pkey", PKEY,
                "-d", "DUMP",
                "-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = 0l;

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_p_fail_test() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{
                "-pkey", "junk",
                "-d", "DUMP",
                "-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.PASSWORD_DECRYPT_ISSUE.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_p_test_02() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{
                "-pkey", "test",
                "-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.PASSWORD_DECRYPT_ISSUE.getLong();

        // As long as failure isn't about decrypt.
        assertFalse("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

}