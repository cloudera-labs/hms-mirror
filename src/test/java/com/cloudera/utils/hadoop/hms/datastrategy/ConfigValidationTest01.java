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

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class ConfigValidationTest01 extends MirrorTestBase {

    @Before
    public void init() throws Exception {
        super.init(HDP2_CDP);
        dataSetup01();
    }

    @Test
    public void test_so() throws IOException {
        DataState.getInstance().setConfiguration(HDP2_CDP);

        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                "-d", "SCHEMA_ONLY",
                "-f",
                "-o", outputDir,
                "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.LEGACY_TO_NON_LEGACY.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_STRATEGY.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

    @Test
    public void test_acid_sql_da_distcp_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao",
                "-da",
                "--distcp",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        long check = MessageCode.SQL_ACID_DA_DISTCP_WO_EXT_WAREHOUSE.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_STRATEGY.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);

    }

    @Test
    public void test_acid_sql_distcp_leg() {
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SQL", "-db", DataState.getInstance().getWorking_db(),
                "-mao",
                "--distcp",
                "-ewd", "/warehouse/external",
                "-o", outputDir, "-cfg", DataState.getInstance().getConfiguration()};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);

        long check = MessageCode.SQL_DISTCP_ONLY_W_DA_ACID.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_STRATEGY.getLong();
//        check = check | MessageCode.STORAGE_MIGRATION_REQUIRED_WAREHOUSE_OPTIONS.getLong();

        assertTrue("Return Code Failure: " + rtn + " doesn't match: " + check, rtn == check);
    }

}