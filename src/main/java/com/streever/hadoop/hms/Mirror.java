package com.streever.hadoop.hms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.stage.CreateDatabases;
import com.streever.hadoop.hms.stage.GetTableMetadata;
import com.streever.hadoop.hms.stage.GetTables;
import com.streever.hadoop.hms.stage.Metadata;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class Mirror {
    private static Logger LOG = LogManager.getLogger(Mirror.class);
    private ScheduledExecutorService threadPool;

    private String[] databases = null;
    private Config config = null;
    private String configFile = null;
    private String reportOutputDir = null;
    private Stage stage = null;

    public void init(String[] args) {

        Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hive-mirror", options);
            System.exit(-1);
        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            System.out.println(ReportingConf.substituteVariables("v.${Implementation-Version}"));
            formatter.printHelp("hive-mirror", options);
            System.exit(-1);
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        databases = cmd.getOptionValues("db");

        if (cmd.hasOption("m")) {
            stage = Stage.METADATA;
            LOG.info("Running METADATA");
        } else if (cmd.hasOption("s")) {
            stage = Stage.STORAGE;
            LOG.info("Running STORAGE");
        } else {
            throw new RuntimeException("Need to specify a 'stage'");
        }

        // Initialize with config and output directory.
        if (cmd.hasOption("cfg")) {
            configFile = cmd.getOptionValue("cfg");
        } else {
            configFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/cfg/default.yaml";
        }

        if (cmd.hasOption("o")) {
            reportOutputDir = cmd.getOptionValue("o");
        } else {
            reportOutputDir = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/reports";
        }
        File reportDir = new File(reportOutputDir);
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }

        File cfgFile = new File(configFile);
        if (!cfgFile.exists()) {
            throw new RuntimeException("Couldn't locate configuration file: " + configFile);
        }

        LOG.info("Check log '" + System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/logs/hms-mirror.log'" +
                " for progress.");

        try {
            System.out.println("Using Config: " + configFile);
            String yamlCfgFile = FileUtils.readFileToString(cfgFile, Charset.forName("UTF-8"));
            config = mapper.readerFor(Config.class).readValue(yamlCfgFile);
        } catch (Throwable t) {
            t.printStackTrace();
        }

        if (cmd.hasOption("dr")) {
            LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            LOG.info("DRY-RUN has been set.  No ACTIONS will be performed, the process output will be recorded in the log.");
            LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            config.setDryrun(Boolean.TRUE);
        }

        ConnectionPools connPools = new ConnectionPools();
        connPools.addHiveServer2(Environment.LOWER, config.getCluster(Environment.LOWER).getHiveServer2());
        connPools.addHiveServer2(Environment.UPPER, config.getCluster(Environment.UPPER).getHiveServer2());
        connPools.init();

        config.getCluster(Environment.LOWER).setPools(connPools);
        config.getCluster(Environment.UPPER).setPools(connPools);


    }

    protected ScheduledExecutorService getThreadPool() {
        if (threadPool == null) {
            threadPool = Executors.newScheduledThreadPool(config.getParallelism());
        }
        return threadPool;
    }

    public void start() {
        Conversion conversion = null;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        switch (stage) {
            case METADATA:
                conversion = runMetadata();
                try {
                    String reportFileStr = reportOutputDir + System.getProperty("file.separator") + "hms-mirror-METADATA-" + df.format(new Date()) + ".md";
                    FileWriter reportFile = new FileWriter(reportFileStr);
                    reportFile.write(conversion.toReport());
                    reportFile.close();
                    LOG.info("Status Report of 'hms-mirror' is here: " + reportFileStr.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            case STORAGE:
                LOG.info("WIP");
                // Need to run the METADATA process first to ensure the schemas are CURRENT.
                // Then run the STORAGE (transfer) stage.
                // NOTE: When the hcfsNamespace is the same between the clusters, that means they are using the
                //          same location (cloud storage) between the clusters.
                //          In this case, don't move any data, BUT we do need to change the source and target table
                //              definitions to show that legacy managed traits transfer to the upper cluster.
                //              So, for the lower cluster the table should be converted to EXTERNAL and the UPPER
                //                  cluster should be set to 'external.table.purge'='true'
                break;
        }
        if (conversion != null) {

        }
    }

    public Conversion runMetadata() {
        Date startTime = new Date();
        LOG.info(">>>>>>>>>>> Start Processing for databases: " + Arrays.toString((databases)));
        Conversion conversion = new Conversion();

        List<ScheduledFuture> gtf = new ArrayList<ScheduledFuture>();
        for (String database : databases) {
            DBMirror dbMirror = conversion.addDatabase(database);
            GetTables gt = new GetTables(config, dbMirror);
            gtf.add(getThreadPool().schedule(gt, 1, TimeUnit.MILLISECONDS));
        }

        // Need to Create LOWER/UPPER Cluster Db(s).
        CreateDatabases transitionCreateDatabases = new CreateDatabases(config, conversion, Boolean.TRUE);
        CreateDatabases createDatabases = new CreateDatabases(config, conversion, Boolean.FALSE);

        gtf.add(getThreadPool().schedule(transitionCreateDatabases, 1, TimeUnit.MILLISECONDS));
        gtf.add(getThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));

        // When the tables have been gathered, complete the process for the METADATA Stage.
        while (true) {
            boolean check = true;
            for (ScheduledFuture sf : gtf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
            }
            if (check)
                break;
        }

        LOG.info(">>>>>>>>>>> Getting Table Metadata");
        List<ScheduledFuture> tmdf = new ArrayList<ScheduledFuture>();
        Set<String> collectedDbs = conversion.getDatabases().keySet();
        for (String database : collectedDbs) {
            DBMirror dbMirror = conversion.getDatabase(database);
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                if (!tblMirror.isTransactional()) {
                    GetTableMetadata tmd = new GetTableMetadata(config, dbMirror, tblMirror);
                    tmdf.add(getThreadPool().schedule(tmd, 1, TimeUnit.MILLISECONDS));
                }
            }
        }

        while (true) {
            boolean check = true;
            for (ScheduledFuture sf : tmdf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
            }
            if (check)
                break;
        }

        LOG.info(">>>>>>>>>>> Building/Starting Transition.");
        List<ScheduledFuture> mdf = new ArrayList<ScheduledFuture>();
//        Set<String> collectedDbs = conversion.getDatabases().keySet();
        for (String database : collectedDbs) {
            DBMirror dbMirror = conversion.getDatabase(database);
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                if (!tblMirror.isTransactional()) {
                    Metadata md = new Metadata(config, dbMirror, tblMirror);
                    mdf.add(getThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
                }
            }
        }

        LOG.info(">>>>>>>>>>> Starting Transfer.");

        while (true) {
            boolean check = true;
            for (ScheduledFuture sf : mdf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
            }
            if (check)
                break;
        }

        getThreadPool().shutdown();
        LOG.info("==============================");
        LOG.info(conversion.toString());
        LOG.info("==============================");
        Date endTime = new Date();
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.CEILING);
        LOG.info("METADATA Completed in: " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");

        return conversion;
    }

    private Options getOptions() {
        // create Options object
        Options options = new Options();

        Option stageOne = new Option("m", "metastore", false, "Run HMS-Mirror Metadata");
        Option stageTwo = new Option("s", "storage", false, "Run HMS-Mirror Storage");

        OptionGroup stageGroup = new OptionGroup();
        stageGroup.addOption(stageOne);
        stageGroup.addOption(stageTwo);

        stageGroup.setRequired(true);
        options.addOptionGroup(stageGroup);

        Option helpOption = new Option("h", "help", false,
                "Help");
        helpOption.setRequired(false);
        options.addOption(helpOption);

        Option outputOption = new Option("o", "output-dir", false,
                "Output Directory (default: $HOME/.hms-mirror/reports/hms-mirror-<stage>-<timestamp>.md");
        outputOption.setRequired(false);
        options.addOption(outputOption);

        Option dryrunOption = new Option("dr", "dry-run", false,
                "No actions are performed, just the output of the commands in the logs.");
        dryrunOption.setRequired(false);
        options.addOption(dryrunOption);

        Option dbOption = new Option("db", "database", true,
                "Comma separated list of Databases (upto 100).");
        dbOption.setValueSeparator(',');
        dbOption.setArgs(100);
        dbOption.setRequired(true);
        options.addOption(dbOption);

        Option cfgOption = new Option("cfg", "config", true,
                "Config with details for the HMS-Mirror.  Default: $HOME/.hms-mirror/cfg/default.yaml");
        cfgOption.setRequired(false);
        options.addOption(cfgOption);

        return options;
    }

    public static void main(String[] args) {
        Mirror mirror = new Mirror();
        LOG.info("===================================================");
        LOG.info("Running: hms-mirror " + ReportingConf.substituteVariables("v.${Implementation-Version}"));
        LOG.info("===================================================");
        mirror.init(args);
        mirror.start();
    }
}
