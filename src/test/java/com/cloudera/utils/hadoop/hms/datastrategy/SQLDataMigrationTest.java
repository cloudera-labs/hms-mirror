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

package com.cloudera.utils.hadoop.hms.datastrategy;

import com.cloudera.utils.hadoop.hms.DataState;
import com.cloudera.utils.hadoop.hms.Mirror;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SQLDataMigrationTest extends MirrorTestBase {

    @Before
    public void init() throws Exception {
        super.init(CDP_CDP);
        dataSetup01();
    }

    @Test
    public void test_acid_sql() {
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

    @Test
    public void test_acid_sql_all() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-ma",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_cs() {
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
    public void test_acid_sql_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-is", intermediate_storage,
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

    @Test
    public void test_acid_sql_cs_w() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-cs", common_storage,
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_da() {
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
    public void test_acid_sql_da_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-da", "-dc",
                "-ewd", "/warehouse/tablespace/external/hive",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_da_ip() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-da", "-ip",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_da_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "-cs", common_storage, "-da", "-o", outputDir,
                "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_w_all() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-ma",
                "-wd", "/warehouse/managed", "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_sql() {
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

    @Test
    public void test_sql_is() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-sql", "-is", "s3a://my_is_bucket/somewhere/here",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

}