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

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.DataState;
import com.cloudera.utils.hadoop.hms.Mirror;
import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.cloudera.utils.hadoop.hms.TestSQL.*;
import static com.cloudera.utils.hadoop.hms.TestSQL.TBL_INSERT;

public class MirrorTestBase {

    protected static final String HDP2_CDP = "default.yaml.hdp2-cdp";
    protected static final String CDP_CDP = "default.yaml.cdp-cdp";
    protected static final String CDP = "default.yaml.cdp";
    protected static final String CDP_ENCRYPT = "default.yaml.cdp.encrypted";
    protected static final String CDP_HDP2 = "default.yaml.cdp-hdp2";

    protected static final String[] execArgs = {"-e", "--accept"};

    protected static String homedir = System.getProperty("user.home");
    protected static String separator = System.getProperty("file.separator");

    protected String intermediate_storage = "s3a://my_is_bucket";
    protected String common_storage = "s3a://my_cs_bucket";
    protected String outputDirBase = null;

    private final String fieldCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    @AfterClass
    public static void tearDownClass() throws Exception {
//        dataCleanup(DATACLEANUP.BOTH);
    }

    public void init(String cfg) throws Exception {
        setUp(cfg);
    }

    @After
    public void tearDown() throws Exception {
        dataCleanup(DATACLEANUP.RIGHT);
    }

    protected static String[] toExecute(String[] one, String[] two, boolean forceExecute) {
        String[] rtn = one;
        if (DataState.getInstance().isExecute() || forceExecute) {
            rtn = new String[one.length + two.length];
            System.arraycopy(one, 0, rtn, 0, one.length);
            System.arraycopy(two, 0, rtn, one.length, two.length);
        }
        if (DataState.getInstance().getTable_filter() != null) {
            String[] tf = {"-tf", DataState.getInstance().getTable_filter()};
            String[] tfRtn = new String[rtn.length + tf.length];
            System.arraycopy(rtn, 0, tfRtn, 0, rtn.length);
            System.arraycopy(tf, 0, tfRtn, rtn.length, tf.length);
            rtn = tfRtn;
        }
        return rtn;
    }

    public Boolean dataSetup01() {
        if (!DataState.getInstance().isDataCreated("dataset01")) {
            String nameofCurrMethod = new Throwable()
                    .getStackTrace()[0]
                    .getMethodName();

            String outputDir = outputDirBase + nameofCurrMethod;

            String[] args = new String[]{"-d", "STORAGE_MIGRATION", "-smn", "s3a://something_not_relevant",
                    "-wd", "/hello", "-ewd", "/hello-ext",
                    "-db", DataState.getInstance().getWorking_db(), "-o", outputDir,
                    "-cfg", DataState.getInstance().getConfiguration()};
            args = toExecute(args, execArgs, Boolean.TRUE);

            List<Pair> leftSql = new ArrayList<Pair>();
            build_use_db(leftSql);

            List<String[]> dataset = null;
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(2, 200, null);
            }
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, TBL_INSERT, dataset, leftSql, new String[]{"acid_01", "2"});
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(2, 400, null);
            }
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, TBL_INSERT, dataset, leftSql, new String[]{"acid_02", "6"});
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(3, 400, null);
            }
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS_PARTITIONED, TBL_INSERT_PARTITIONED, dataset, leftSql, new String[]{"acid_03", "6"});
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(2, 2000, 500);
            }
            build_n_populate(CREATE_EXTERNAL_TBL_PARTITIONED, TBL_INSERT_PARTITIONED, dataset, leftSql, new String[]{"ext_part_01"});
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(2, 2000, null);
            }
            build_n_populate(CREATE_EXTERNAL_TBL, TBL_INSERT, dataset, leftSql, new String[]{"ext_part_02"});
            build_n_populate(CREATE_LEGACY_MNGD_TBL, TBL_INSERT, dataset, leftSql, new String[]{"legacy_mngd_01"});

            Mirror cfgMirror = new Mirror();
            long rtn = cfgMirror.setupSqlLeft(args, leftSql);
            DataState.getInstance().setDataCreated("dataset01", Boolean.TRUE);
        }
        return Boolean.TRUE;
    }

    public enum DATACLEANUP {
        LEFT, RIGHT, BOTH
    }

    protected static Boolean dataCleanup(DATACLEANUP datacleanup) {
        // Only Cleanup Data if Clean and Execute Flags are set to prevent downstream issues with test
        // dataset.
        if (DataState.getInstance().isCleanUp() && DataState.getInstance().isExecute()) {
            String nameofCurrMethod = new Throwable()
                    .getStackTrace()[0]
                    .getMethodName();

            String outputDir = homedir + separator + "hms-mirror-reports" + separator + MirrorTestBase.class.getSimpleName() +
                    separator + nameofCurrMethod;

            String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
                    "-d", "DUMP",
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
            Config cfg = Context.getInstance().getConfig();
            String ns = null;
            switch (datacleanup) {
                case LEFT:
                    ns = cfg.getCluster(Environment.LEFT).getHcfsNamespace();
                    rtn = cfgMirror.setupSqlLeft(args, leftSql);
                    break;
                case RIGHT:
                    ns = cfg.getCluster(Environment.RIGHT).getHcfsNamespace();
                    // Need to figure out which dataset to reset.
//                    DataState.getInstance().setDataCreated();
                    rtn = cfgMirror.setupSqlRight(args, rightSql);
                    break;
                case BOTH:
                    String lns = cfg.getCluster(Environment.LEFT).getHcfsNamespace();
                    String rns = cfg.getCluster(Environment.RIGHT).getHcfsNamespace();
                    rtn = cfgMirror.setupSql(args, leftSql, rightSql);
                    break;
            }
        }
        return Boolean.TRUE;
    }

    protected void build_n_populate(String tableDefTemplate, String insertTemplate,
                                    List<String[]> dataset, List<Pair> targetPairList, Object[] opts) {
        MessageFormat mf = new MessageFormat("US");
        String tableCreate = MessageFormat.format(tableDefTemplate, opts);
        String tableName = (String) opts[0];
        Pair createPair = new Pair("Create table: " + tableName, tableCreate);
        targetPairList.add(createPair);
        if (dataset != null) {
            StringBuilder insertValuesSb = new StringBuilder();
            Iterator iter = dataset.iterator();
            while (iter.hasNext()) {
                String[] row = (String[]) iter.next();
                row = (String[]) iter.next();
                insertValuesSb.append("(");
                for (int i = 0; i < row.length; i++) {
                    insertValuesSb.append("\"").append(row[i]).append("\"");
                    if (i < row.length - 1) {
                        insertValuesSb.append(",");
                    }
                }
                insertValuesSb.append(")");
                if (iter.hasNext()) {
                    insertValuesSb.append(",");
                }
            }

            String insert = MessageFormat.format(insertTemplate, tableName, insertValuesSb.toString());
            Pair insertPair = new Pair("Insert into table", insert);
            targetPairList.add(insertPair);
        }

    }

    protected void build_use_db(List<Pair> sqlPairList) {
        String createDb = MessageFormat.format(MirrorConf.CREATE_DB, DataState.getInstance().getWorking_db());
        Pair r02p = new Pair("CREATE DB: " + DataState.getInstance().getWorking_db(), createDb);
        String useDb = MessageFormat.format(MirrorConf.USE, DataState.getInstance().getWorking_db());
        Pair r03p = new Pair("Use DB: " + DataState.getInstance().getWorking_db(), useDb);
        sqlPairList.add(r02p);
        sqlPairList.add(r03p);
    }

    protected List<String[]> getDataset(Integer width, Integer count, Integer partitions) {
        List<String[]> rtn = new ArrayList<String[]>();
        Integer realWidth = width;
        if (partitions != null) {
            realWidth += 1;
        }
        for (int i = 0; i < count; i++) {
            String[] record = null;
            record = new String[realWidth];
            for (int j = 0; j < realWidth; j++) {
                String field = null;
                if (j < width) {
                    field = RandomStringUtils.random(15, fieldCharacters);
                } else {
                    field = Integer.toString(RandomUtils.nextInt(0, partitions));
                }
                record[j] = field;
            }
            rtn.add(record);
        }
        return rtn;
    }

    public void setUp(String configLocation) throws Exception {
        DataState.getInstance().setConfiguration(configLocation);

        // Override default db for test run.
        String lclWorkingDb = System.getenv("DB");
        if (lclWorkingDb != null) {
            DataState.getInstance().setWorking_db(lclWorkingDb);
            DataState.getInstance().setDataCreated(lclWorkingDb, Boolean.TRUE);
            DataState.getInstance().setSkipAdditionDataCreation(Boolean.TRUE);
            DataState.getInstance().setCleanUp(Boolean.FALSE);
        }

        // Set the execute flags for test suite.
        String execStr = System.getenv("EXECUTE");
        if (execStr != null) {
            DataState.getInstance().setExecute(Boolean.valueOf(execStr));
        }

        String cleanStr = System.getenv("CLEANUP");
        if (cleanStr != null) {
            DataState.getInstance().setCleanUp(Boolean.valueOf(cleanStr));
        }

        String tfStr = System.getenv("TF");
        if (tfStr != null) {
            DataState.getInstance().setTable_filter(tfStr);
        }

        // Override intermediate_storage for test run.
        String lclIntermediate_storage = System.getenv("IS");
        if (lclIntermediate_storage != null) {
            intermediate_storage = lclIntermediate_storage;
        }

        // Override common_storage for test run.
        String lclcommon_storage = System.getenv("CS");
        if (lclcommon_storage != null) {
            common_storage = lclcommon_storage;
        }

        // Override storage migration location for test run.
        String smn_storage = System.getenv("SMN");
        if (smn_storage != null) {
            common_storage = smn_storage;
        }

        // Override storage migration location for test run.
        String populate_data = System.getenv("PD");
        if (populate_data != null) {
            DataState.getInstance().setPopulate(Boolean.valueOf(populate_data));
        }


        String cfg = System.getenv("CFG");
        if (cfg != null) {
            DataState.getInstance().setConfiguration(cfg);
        }

        outputDirBase = homedir + separator + "hms-mirror-reports" + separator +
                DataState.getInstance().getUnique() + separator +
                getClass().getSimpleName() + separator;

    }


}
