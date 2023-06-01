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
import com.cloudera.utils.hadoop.hms.mirror.MessageCode;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SchemaMigrationTest extends MirrorTestBase {

    @Before
    public void init() throws Exception {
        super.init(HDP2_CDP);
        dataSetup01();
    }

    @Test
    public void test_acid_b_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
                "-mao", "4",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
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
    public void test_acid_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
                "-mao",
                "-rdl", "-wd", "/warehouse/tablespace/managed/hive",
                "-ewd", "/warehouse/tablespace/external/hive",
                "--distcp",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_common() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "COMMON", "-db", DataState.getInstance().getWorking_db(),
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
    public void test_dbo() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-dbo",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);

    }

    @Test
    public void test_dump() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "DUMP", "-db", DataState.getInstance().getWorking_db(),
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
    public void test_linked() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "LINKED", "-db", DataState.getInstance().getWorking_db(),
                "-sql",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }


//    @Test
//    public void test_reset_right() {
//        reset();
//    }

    @Test
    public void test_so() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_so_distcp() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "--distcp",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_so_existing() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);

        // Testing for existing schemas.
        outputDir = outputDir + "/2";
        args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        rtn = 0;
        mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);

    }

    @Test
    public void test_so_cs() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-cs", "s3a://my_common_storage",
                "--distcp",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_so_cs_rdl() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-cs", "s3a://my_common_storage",
                "-rdl",
                "--distcp",
                "-wd","/warehouse/managed",
                "-ewd","/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        int check = 0;
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_so_rdl_dc_w() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-rdl",
                "--distcp",
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

    @Test
    public void test_so_rdl_w_dc() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-rdl",
                "-dc",
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

    @Test
    public void test_so_ro() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-ro",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.RO_DB_DOESNT_EXIST.getLong();
        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

}