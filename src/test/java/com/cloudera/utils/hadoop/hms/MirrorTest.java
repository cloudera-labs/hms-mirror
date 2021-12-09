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

import org.junit.Before;
import org.junit.Test;

public class MirrorTest {

    private String homedir = null;

    @Before
    public void setUp() throws Exception {
        homedir = System.getProperty("user.home");
    }

    @Test
    public void test_so_ro() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_ro";
        String[] args = new String[]{"-db", "tpcds_bin_partitioned_orc_10", "-ro", "-sql",  "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_so_ro_tf() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_ro_tf";
        String[] args = new String[]{"-db", "tpcds_bin_partitioned_orc_10", "-tf", "call_center|store_sales", "-ro", "-sql",  "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_so_ro_sync() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/so_ro_sync";
        String[] args = new String[]{"-db", "tpcds_bin_partitioned_orc_10", "-ro", "-sync", "-sql", "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_dump() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/dump";
        String[] args = new String[]{"-d", "DUMP", "-db", "tpcds_bin_partitioned_orc_10", "-sql", "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_linked() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/linked";
        String[] args = new String[]{"-d", "LINKED", "-db", "tpcds_bin_partitioned_orc_10", "-sql", "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_common() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/common";
        String[] args = new String[]{"-d", "COMMON", "-db", "tpcds_bin_partitioned_orc_10", "-sql", "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_sql() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/sql";
        String[] args = new String[]{"-d", "SQL", "-db", "tpcds_bin_partitioned_orc_10", "-sql", "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_hybrid() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/hybrid";
        String[] args = new String[]{"-d", "HYBRID", "-db", "tpcds_bin_partitioned_orc_10", "-sql", "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

    @Test
    public void test_exp_imp() {
        String outputDir = homedir + System.getProperty("file.separator") + "hms-mirror-reports/exp_imp";
        String[] args = new String[]{"-d", "EXPORT_IMPORT", "-db", "tpcds_bin_partitioned_orc_10", "-sql", "-o", outputDir};

        Mirror mirror = new Mirror();
        mirror.init(args);
        mirror.doit();
    }

}