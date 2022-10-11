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

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MirrorTest01 extends MirrorTestBase {

    @Test
    public void test_spot_test() {
//        reset();
        String nameofCurrMethod = new Throwable()
                .getStackTrace()[0]
                .getMethodName();

        String outputDir = outputDirBase + nameofCurrMethod;

        String[] args = new String[]{"-d", "SCHEMA_ONLY", "-db", DataState.getInstance().getWorking_db(),
                "-tf", "hello_manager",
                "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
//        String[] args = new String[]{"-d", "SQL", "-db", working_db, "-mao", "4", "-da", "-r", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
//        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", working_db, "-mao", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + CDP_CDP};
//        String[] args = new String[]{"-db", working_db, "-ma", "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
//        String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(), "-d", "STORAGE_MIGRATION",
//                "-smn", "ofs://OHOME90",
//                "-f",
//                "-wd", "/warehouse/managed",
//                "-ewd", "/warehouse/external",
//                "-o", outputDir, "-cfg", System.getProperty("user.home") + "/.hms-mirror/cfg/" + HDP2_CDP};
        args = toExecute(args, execArgs, Boolean.FALSE);

        long rtn = 0;
        Mirror mirror = new Mirror();
        rtn = mirror.go(args);
        assertTrue("Return Code Failure: " + rtn, rtn == 0);
    }

}
