package com.streever.hadoop.hms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.stage.*;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
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
    private String reportOutputFile = null;
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

        if (cmd.hasOption("f")) {
            reportOutputFile = cmd.getOptionValue("f");
        } else {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            reportOutputFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/reports/" +
                    "hms-mirror-METADATA-" + df.format(new Date()) + ".md";
        }

        String reportPath = reportOutputFile.substring(0,reportOutputFile.lastIndexOf(System.getProperty("file.separator")));
        File reportPathDir = new File(reportPath);
        if (!reportPathDir.exists()) {
            reportPathDir.mkdirs();
        }

        File reportFile = new File(reportOutputFile);

        // Test file to ensure we can write to it for the report.
        try {
            new FileOutputStream(reportFile).close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
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

        if (cmd.hasOption("db")) {
            String[] databases = cmd.getOptionValues("db");
            if (databases != null)
                config.setDatabases(databases);
        }

        if (cmd.hasOption("dbRegEx")) {
            config.setDbRegEx(cmd.getOptionValue("dbRegEx"));
        }

        if (cmd.hasOption("tf")) {
            config.setTblRegEx(cmd.getOptionValue("tf"));
        }

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

    public void doit() {
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
                // Make / override any conflicting settings.
                Storage.fixConfig(config);
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
//            String reportFileStr = reportOutputFile;
            FileWriter reportFile = new FileWriter(reportOutputFile);
            reportFile.write(conversion.toReport(config));
            reportFile.close();
            LOG.info("Status Report of 'hms-mirror' is here: " + reportOutputFile.toString());
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
                Storage sd = new Storage(config, dbMirror, tblMirror);
                sdf.add(config.getStorageThreadPool().schedule(sd, 1, TimeUnit.MILLISECONDS));
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
                if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER))) {
                    Metadata md = new Metadata(config, dbMirror, tblMirror);
                    mdf.add(config.getMetadataThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
                } else {
                    tblMirror.addIssue("ACID Table not supported for METADATA phase");
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

        Option outputOption = new Option("f", "output-file", true,
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
//        Option dbRegExOption = new Option("dbRegEx", "database-regex", true,
//                "Search RegEx of Database names to include in processing");
        OptionGroup dbGroup = new OptionGroup();
        dbGroup.addOption(dbOption);
//        dbGroup.addOption(dbRegExOption);
        dbGroup.setRequired(true);
        options.addOptionGroup(dbGroup);

        Option tableFilterOption = new Option("tf", "table-filter", true, "Filter tables with name matching RegEx");
        tableFilterOption.setRequired(false);
        options.addOption(tableFilterOption);

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
        mirror.doit();
        System.exit(0);
    }
}
