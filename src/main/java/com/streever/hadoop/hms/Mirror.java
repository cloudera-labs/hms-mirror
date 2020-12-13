package com.streever.hadoop.hms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.stage.*;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class Mirror {
    private static Logger LOG = LogManager.getLogger(Mirror.class);
//    private ScheduledExecutorService metadataThreadPool;
//    private ScheduledExecutorService storageThreadPool;

//    private String[] databases = null;
    private Config config = null;
    private String configFile = null;
    private String reportOutputDir = null;
//    private Stage stage = null;

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

        if (cmd.hasOption("m")) {
            config.setStage(Stage.METADATA);
            String mdirective = cmd.getOptionValue("m");
            if (mdirective != null) {
                MetadataConfig.Strategy strategy = MetadataConfig.Strategy.valueOf(mdirective.toUpperCase(Locale.ROOT));
                config.getMetadata().setStrategy(strategy);
            }
            LOG.info("Running METADATA");
        } else if (cmd.hasOption("s")) {
            config.setStage(Stage.STORAGE);
            String sdirective = cmd.getOptionValue("s");
            if (sdirective != null) {
                StorageConfig.Strategy strategy = StorageConfig.Strategy.valueOf(sdirective.toUpperCase(Locale.ROOT));
                config.getStorage().setStrategy(strategy);
            }
            LOG.info("Running STORAGE");
        }

        if (config.getStage() == null) {
            throw new RuntimeException("Stage (METADATA|STORAGE) has not been specified.");
        }

        String[] databases = cmd.getOptionValues("db");
        if (databases != null)
            config.setDatabases(databases);

        if (config.getDatabases() == null || config.getDatabases().length == 0) {
            LOG.error("No databases specified");
            throw new RuntimeException("No databases specified");
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

    public void start() {
        Conversion conversion = new Conversion();
        Date startTime = new Date();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

        Setup setup = new Setup(config, conversion);
        setup.run();

        switch (config.getStage()) {
            case METADATA:
                conversion = runMetadata(conversion);
                break;
            case STORAGE:
                LOG.info("WIP");
                // We run the Metadata
                conversion = runStorage(conversion);
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
        try {
            String reportFileStr = reportOutputDir + System.getProperty("file.separator") + "hms-mirror-METADATA-" + df.format(new Date()) + ".md";
            FileWriter reportFile = new FileWriter(reportFileStr);
            reportFile.write(conversion.toReport());
            reportFile.close();
            LOG.info("Status Report of 'hms-mirror' is here: " + reportFileStr.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Date endTime = new Date();
            DecimalFormat decf = new DecimalFormat("#.###");
            decf.setRoundingMode(RoundingMode.CEILING);
            LOG.info("HMS-Mirror: Completed in " +
                    decf.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");

        }
    }

//    public Conversion setup(Conversion conversion) {
//        Date startTime = new Date();
//        LOG.info("GATHERING METADATA: Start Processing for databases: " + Arrays.toString((databases)));
//
//        List<ScheduledFuture> gtf = new ArrayList<ScheduledFuture>();
//        for (String database : databases) {
//            DBMirror dbMirror = conversion.addDatabase(database);
//            GetTables gt = new GetTables(config, dbMirror);
//            gtf.add(config.getMetadataThreadPool().schedule(gt, 1, TimeUnit.MILLISECONDS));
//        }
//
//        // Need to Create LOWER/UPPER Cluster Db(s).
//        if (config.getMetadata().getStrategy().equals(MetadataConfig.Strategy.TRANSITION)) {
//            CreateDatabases transitionCreateDatabases = new CreateDatabases(config, conversion, Boolean.TRUE);
//            gtf.add(config.getMetadataThreadPool().schedule(transitionCreateDatabases, 1, TimeUnit.MILLISECONDS));
//        }
//
//        CreateDatabases createDatabases = new CreateDatabases(config, conversion, Boolean.FALSE);
//        gtf.add(config.getMetadataThreadPool().schedule(createDatabases, 1, TimeUnit.MILLISECONDS));
//
//        // When the tables have been gathered, complete the process for the METADATA Stage.
//        while (true) {
//            boolean check = true;
//            for (ScheduledFuture sf : gtf) {
//                if (!sf.isDone()) {
//                    check = false;
//                    break;
//                }
//            }
//            if (check)
//                break;
//        }
//
//        LOG.info(">>>>>>>>>>> Getting Table Metadata");
//        List<ScheduledFuture> tmdf = new ArrayList<ScheduledFuture>();
//        Set<String> collectedDbs = conversion.getDatabases().keySet();
//        for (String database : collectedDbs) {
//            DBMirror dbMirror = conversion.getDatabase(database);
//            Set<String> tables = dbMirror.getTableMirrors().keySet();
//            for (String table : tables) {
//                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
//                if (!tblMirror.isTransactional()) {
//                    GetTableMetadata tmd = new GetTableMetadata(config, dbMirror, tblMirror);
//                    tmdf.add(config.getMetadataThreadPool().schedule(tmd, 1, TimeUnit.MILLISECONDS));
//                }
//            }
//        }
//
//        while (true) {
//            boolean check = true;
//            for (ScheduledFuture sf : tmdf) {
//                if (!sf.isDone()) {
//                    check = false;
//                    break;
//                }
//            }
//            if (check)
//                break;
//        }
//
//        LOG.info("==============================");
//        LOG.info(conversion.toString());
//        LOG.info("==============================");
//        Date endTime = new Date();
//        DecimalFormat df = new DecimalFormat("#.###");
//        df.setRoundingMode(RoundingMode.CEILING);
//        LOG.info("GATHERING METADATA: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
//
//        return conversion;
//    }

    public Conversion runStorage(Conversion conversion) {
        Date startTime = new Date();
        LOG.info("STORAGE-STAGE: Start Processing for databases: " + Arrays.toString((config.getDatabases())));

        LOG.info(">>>>>>>>>>> Building/Starting Storage.");
        List<ScheduledFuture> sdf = new ArrayList<ScheduledFuture>();

        Set<String> collectedDbs = conversion.getDatabases().keySet();
        for (String database : collectedDbs) {
            DBMirror dbMirror = conversion.getDatabase(database);
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                if (!tblMirror.isTransactional()) {
                    Storage sd = new Storage(config, dbMirror, tblMirror);
                    sdf.add(config.getStorageThreadPool().schedule(sd, 1, TimeUnit.MILLISECONDS));
                }
            }
        }

        LOG.info(">>>>>>>>>>> Starting Transfer.");

        while (true) {
            boolean check = true;
            for (ScheduledFuture sf : sdf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
            }
            if (check)
                break;
        }

        config.getStorageThreadPool().shutdown();

        LOG.info("==============================");
        LOG.info(conversion.toString());
        LOG.info("==============================");
        Date endTime = new Date();
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.CEILING);
        LOG.info("STORAGE-STAGE: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");

        return conversion;
    }

    public Conversion runMetadata(Conversion conversion) {
        Date startTime = new Date();
        LOG.info("METADATA-STAGE: Start Processing for databases: " + Arrays.toString((config.getDatabases())));
//        Conversion conversion = new Conversion();

        LOG.info(">>>>>>>>>>> Building/Starting Transition.");
        List<ScheduledFuture> mdf = new ArrayList<ScheduledFuture>();

        Set<String> collectedDbs = conversion.getDatabases().keySet();
        for (String database : collectedDbs) {
            DBMirror dbMirror = conversion.getDatabase(database);
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                if (!tblMirror.isTransactional()) {
                    Metadata md = new Metadata(config, dbMirror, tblMirror);
                    mdf.add(config.getMetadataThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
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

        config.getMetadataThreadPool().shutdown();

        LOG.info("==============================");
        LOG.info(conversion.toString());
        LOG.info("==============================");
        Date endTime = new Date();
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.CEILING);
        LOG.info("METADATA-STAGE: Completed in " + df.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");

        return conversion;
    }

    private Options getOptions() {
        // create Options object
        Options options = new Options();

        Option metadataStage = new Option("m", "metastore", true, "Run HMS-Mirror Metadata");
        metadataStage.setOptionalArg(Boolean.TRUE);
        metadataStage.setArgName("DIRECT(default)|TRANSITION");
        Option storageStage = new Option("s", "storage", true, "Run HMS-Mirror Storage ('sql|export' only supported options currently)");
        storageStage.setArgName("SQL|EXPORT_IMPORT|HYBRID|DISTCP");
        storageStage.setOptionalArg(Boolean.TRUE);

        OptionGroup stageGroup = new OptionGroup();
        stageGroup.addOption(metadataStage);
        stageGroup.addOption(storageStage);

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
