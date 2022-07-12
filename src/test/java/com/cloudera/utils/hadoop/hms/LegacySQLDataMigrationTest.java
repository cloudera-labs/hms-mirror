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
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static org.junit.Assert.assertTrue;

public class LegacySQLDataMigrationTest extends MirrorTestBase {

    @AfterClass
    public static void tearDownClass() throws Exception {
        dataCleanup(DATACLEANUP.BOTH);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DataState.getInstance().setConfiguration(HDP2_CDP);
        dataSetup01();
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(DATACLEANUP.RIGHT);
    }

// `-r` Feature removed for now..
//    @Test
//    public void test_acid_sql_da_cs_r_all_leg() {
//        String nameofCurrMethod = new Throwable()
//                .getStackTrace()[0]
//                .getMethodName();
//
//        String outputDir = outputDirBase + nameofCurrMethod;
//
//        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
//                "-ma", "-da", "-r", "-cs", common_storage,
//                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
//        args = toExecute(args, execArgs, Boolean.FALSE);
//
//        long rtn = 0;
//        Mirror mirror = new Mirror();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
//    }

    @Test
    public void test_acid_sql_da_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-da",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_da_leg_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-cs", common_storage, "-da",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

// `-r` Feature removed for now..
//    @Test
//    public void test_acid_sql_da_r_all_leg() {
//        String nameofCurrMethod = new Throwable()
//                .getStackTrace()[0]
//                .getMethodName();
//
//        String outputDir = outputDirBase + nameofCurrMethod;
//
//        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
//                "-ma", "-da", "-r",
//                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
//        args = toExecute(args, execArgs, Boolean.FALSE);
//
//        long rtn = 0;
//        Mirror mirror = new Mirror();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
//    }

// `-r` Feature removed for now..
//    @Test
//    public void test_acid_sql_da_r_leg() {
//        String nameofCurrMethod = new Throwable()
//                .getStackTrace()[0]
//                .getMethodName();
//
//        String outputDir = outputDirBase + nameofCurrMethod;
//
//        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
//                "-mao", "-da", "-r",
//                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
//        args = toExecute(args, execArgs, Boolean.FALSE);
//
//        long rtn = 0;
//        Mirror mirror = new Mirror();
//        rtn = mirror.go(args);
//        int check = 0;
//        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
//    }

    @Test
    public void test_acid_sql_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    // ====
    @Test
    public void test_acid_sql_leg_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-cs", common_storage,
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }


    @Test
    public void test_sql_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    /*
    TODO: Bug...  For EXTERNAL tables the 'transfer' table isn't 'managed' (in legacy) so
            when it's deleted, the data isn't cleaned up.
            Currently, the intermediate storage location is unique and can be
              cleaned up after the fact.
     */
    @Test
    public void test_sql_is_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-is", "s3a://my_intermediate_bucket",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_sql_cs_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-cs", "s3a://my_common_bucket",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_sql_rdl_w_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-sql",
                "-rdl",
                "-wd", "/warehouse/managed",
                "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }
}