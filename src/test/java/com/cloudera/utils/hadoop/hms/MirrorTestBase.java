package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import com.cloudera.utils.hadoop.hms.mirror.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
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
    protected static final String CDP_HDP2 = "default.yaml.cdp-hdp2";

    protected static final String[] execArgs = {"-e", "--accept"};

    protected static String homedir = System.getProperty("user.home");
    protected static String separator = System.getProperty("file.separator");

    protected String intermediate_storage = "s3a://my_is_bucket";
    protected String common_storage = "s3a://my_cs_bucket";
    protected String outputDirBase = null;

    private final String fieldCharacters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

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
        if (!DataState.getInstance().isDataCreated()) {
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
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_01", 2, TBL_INSERT, dataset, leftSql);
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(2, 400, null);
            }
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS, "acid_02", 6, TBL_INSERT, dataset, leftSql);
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(3, 400, null);
            }
            build_n_populate(CREATE_LEGACY_ACID_TBL_N_BUCKETS_PARTITIONED, "acid_03", 6, TBL_INSERT_PARTITIONED, dataset, leftSql);
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(2, 2000, 500);
            }
            build_n_populate(CREATE_EXTERNAL_TBL_PARTITIONED, "ext_part_01", null, TBL_INSERT_PARTITIONED, dataset, leftSql);
            if (DataState.getInstance().getPopulate() == null || DataState.getInstance().getPopulate()) {
                dataset = getDataset(2, 2000, null);
            }
            build_n_populate(CREATE_EXTERNAL_TBL, "ext_part_02", null, TBL_INSERT, dataset, leftSql);

            Mirror cfgMirror = new Mirror();
            long rtn = cfgMirror.setupSql(args, leftSql, null);
            DataState.getInstance().setDataCreated(Boolean.TRUE);
        }
        return Boolean.TRUE;
    }

    protected static Boolean dataCleanup(Boolean rightOnly) {
        if (DataState.getInstance().isCleanUp()) {
            String nameofCurrMethod = new Throwable()
                    .getStackTrace()[0]
                    .getMethodName();

            String outputDir = homedir + separator + "hms-mirror-reports" + separator + MirrorTestBase.class.getSimpleName() +
                    separator + nameofCurrMethod;

            String[] args = new String[]{"-db", DataState.getInstance().getWorking_db(),
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

            if (rightOnly) {
                long rtn = cfgMirror.setupSql(args, null, rightSql);
            } else {
                long rtn = cfgMirror.setupSql(args, leftSql, rightSql);
                DataState.getInstance().setDataCreated(Boolean.FALSE);
            }
        }
        return Boolean.TRUE;

    }

    protected void build_n_populate(String tableDefTemplate, String tableName, Integer buckets, String insertTemplate,
                                    List<String[]> dataset, List<Pair> targetPairList) {
        String tableCreate = MessageFormat.format(tableDefTemplate, tableName, buckets);
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
        String dropDb = MessageFormat.format(MirrorConf.DROP_DB, DataState.getInstance().getWorking_db());
        Pair r01p = new Pair("DROP DB: " + DataState.getInstance().getWorking_db(), dropDb);
        String createDb = MessageFormat.format(MirrorConf.CREATE_DB, DataState.getInstance().getWorking_db());
        Pair r02p = new Pair("CREATE DB: " + DataState.getInstance().getWorking_db(), createDb);
        String useDb = MessageFormat.format(MirrorConf.USE, DataState.getInstance().getWorking_db());
        Pair r03p = new Pair("Use DB: " + DataState.getInstance().getWorking_db(), useDb);
        sqlPairList.add(r01p);
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

    @Before
    public void setUp() throws Exception {
        // Override default db for test run.
        String lclWorkingDb = System.getenv("DB");
        if (lclWorkingDb != null) {
            DataState.getInstance().setWorking_db(lclWorkingDb);
            DataState.getInstance().setDataCreated(Boolean.TRUE);
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
