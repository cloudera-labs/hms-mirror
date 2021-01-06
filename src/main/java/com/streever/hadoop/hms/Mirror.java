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

    private Config config = null;
    private String configFile = null;
    private String reportOutputFile = null;
    private Boolean retry = Boolean.FALSE;
    private String dateMarker;

    public String getDateMarker() {
        if (dateMarker == null) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            dateMarker = df.format(new Date());
        }
        return dateMarker;
    }

    public void setDateMarker(String dateMarker) {
        this.dateMarker = dateMarker;
    }

    public void init(String[] args) {

        Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException pe) {
            System.out.println("Missing Arguments: " + pe.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror \nversion:${Implementation-Version}");
            formatter.printHelp(cmdline, options);
            System.exit(-1);
        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror \nversion:${Implementation-Version}");
            formatter.printHelp(cmdline, options);
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

        if (cmd.hasOption("r")) {
            retry = Boolean.TRUE;
        }

        if (cmd.hasOption("m")) {
            config.setStage(Stage.METADATA);
            String mdirective = cmd.getOptionValue("m");
            if (mdirective != null) {
                Strategy strategy = Strategy.valueOf(mdirective.toUpperCase(Locale.ROOT));
                config.getMetadata().setStrategy(strategy);
            }
            LOG.info("Running METADATA");
        } else if (cmd.hasOption("s")) {
            config.setStage(Stage.STORAGE);
            String sdirective = cmd.getOptionValue("s");
            if (sdirective != null) {
                Strategy strategy = Strategy.valueOf(sdirective.toUpperCase(Locale.ROOT));
                config.getStorage().setStrategy(strategy);
            }
            LOG.info("Running STORAGE");
        } else if (cmd.hasOption("c")) {
            config.setStage(Stage.CONVERT);
            String cdirective = cmd.getOptionValue("c");
            if (cdirective != null) {
                Strategy strategy = Strategy.valueOf(cdirective.toUpperCase(Locale.ROOT));
                config.getStorage().setStrategy(strategy);
            }
            LOG.info("Running CONVERT");

        }

        if (cmd.hasOption("f")) {
            reportOutputFile = cmd.getOptionValue("f");
        } else {
            reportOutputFile = System.getProperty("user.home") + System.getProperty("file.separator") +
                    ".hms-mirror/reports/hms-mirror-" + config.getStage() + "-" + getDateMarker() + ".md";
        }

        String reportPath = reportOutputFile.substring(0, reportOutputFile.lastIndexOf(System.getProperty("file.separator")));
        File reportPathDir = new File(reportPath);
        if (!reportPathDir.exists()) {
            reportPathDir.mkdirs();
        }

        File reportFile = new File(reportOutputFile);

        // Ensure the Retry Path is created.
        File retryPath = new File(System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror" +
                System.getProperty("file.separator") + "retry");
        if (!retryPath.exists()) {
            retryPath.mkdirs();
        }

        // Test file to ensure we can write to it for the report.
        try {
            new FileOutputStream(reportFile).close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
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
        Conversion conversion = new Conversion(config);

        // Setup and Start the State Maintenance Routine
        StateMaintenance stateMaintenance = new StateMaintenance(5000, configFile, getDateMarker());

        if (retry) {
            File retryFile = stateMaintenance.getRetryFile();
            if (!retryFile.exists()) {
                LOG.error("Could NOT locate 'retry' file: " + retryFile.getPath());
                throw new RuntimeException("Could NOT locate 'retry' file: " + retryFile.getPath());
            }
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String retryCfgFile = null;
            try {
                retryCfgFile = FileUtils.readFileToString(retryFile, Charset.forName("UTF-8"));
                // Replace Conversion
                conversion = mapper.readerFor(Conversion.class).readValue(retryCfgFile);
                // Replace Config
                config = conversion.getConfig();
            } catch (IOException e) {
                LOG.error("Could NOT read 'retry' file: " + retryFile.getPath(), e);
                throw new RuntimeException("Could NOT read 'retry' file: " + retryFile.getPath(), e);
            }
        }

        // Link the conversion to the state machine.
        stateMaintenance.setConversion(conversion);

        // Setup and Start the Reporter
        Reporter reporter = new Reporter(conversion, 1000);
        reporter.setVariable("config.file", configFile);
        reporter.setVariable("stage", config.getStage().toString());
        // ?
        //        reporter.setVariable("log.file", log4j output file.);
        reporter.setVariable("report.file", reportOutputFile);
        reporter.setVariable("action.script", "none");
        reporter.setRetry(this.retry);
        reporter.start();

        Date startTime = new Date();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

        // Skip Setup if working from 'retry'
        if (!retry) {
            Setup setup = new Setup(config, conversion);
            // TODO: Failure here may not make it to saved state.
            if (setup.collect()) {
                stateMaintenance.saveState();
            } else {
                // Need to delete retry file.
                stateMaintenance.deleteState();
            }
        }

        stateMaintenance.start();

        switch (config.getStage()) {
            case METADATA:
                conversion = runMetadata(conversion);
                stateMaintenance.saveState();
                break;
            case STORAGE:
                // Make / override any conflicting settings.
                Boolean rtn = Storage.fixConfig(config);
                if (rtn) {
                    conversion = runStorage(conversion);
                    stateMaintenance.saveState();
                }
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
            reporter.refresh();
            reporter.stop();
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
            switch (config.getStorage().getStrategy()) {
                case SQL:
                case EXPORT_IMPORT:
                case HYBRID:
                    for (String table : tables) {
                        TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                        switch (tblMirror.getPhaseState()) {
                            case INIT:
                            case STARTED:
                            case ERROR:
                                Storage sd = new Storage(config, dbMirror, tblMirror);
                                sdf.add(config.getStorageThreadPool().schedule(sd, 1, TimeUnit.MILLISECONDS));
                                break;
                            case SUCCESS:
                                LOG.debug("DB.tbl: " + tblMirror.getDbName() + "." + tblMirror.getName() + " was SUCCESSFUL in " +
                                        "previous run.   SKIPPING and adjusting status to RETRY_SKIPPED_PAST_SUCCESS");
                                tblMirror.setPhaseState(PhaseState.RETRY_SKIPPED_PAST_SUCCESS);
                                break;
                            case RETRY_SKIPPED_PAST_SUCCESS:
                                LOG.debug("DB.tbl: " + tblMirror.getDbName() + "." + tblMirror.getName() + " was SUCCESSFUL in " +
                                        "previous run.  SKIPPING");
                                break;
                        }
                    }
                    break;
                case DISTCP:
                    // for distcp, we need to build on two strategies for paths.  DB or TABLE.
                    switch (config.getStorage().getDistcp().getPathStrategy()) {
                        case DB:
                            Storage sd = new Storage(config, dbMirror);
                            sdf.add(config.getStorageThreadPool().schedule(sd, 1, TimeUnit.MILLISECONDS));
                            break;
                        case TABLE:
                            for (String table : tables) {
                                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                                Storage sd1 = new Storage(config, dbMirror, tblMirror);
                                sdf.add(config.getStorageThreadPool().schedule(sd1, 1, TimeUnit.MILLISECONDS));
                            }
                            break;
                    }
                    break;
                case DIRECT:
                case SCHEMA_EXTRACT:
                    break;
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
                switch (tblMirror.getPhaseState()) {
                    case INIT:
                    case STARTED:
                    case ERROR:
                        if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LOWER))) {
                            Metadata md = new Metadata(config, dbMirror, tblMirror);
                            mdf.add(config.getMetadataThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
                        } else {
                            tblMirror.addIssue("ACID Table not supported for METADATA phase");
                            tblMirror.setPhaseState(PhaseState.ERROR);
                        }
                        break;
                    case SUCCESS:
                        LOG.debug("DB.tbl: " + tblMirror.getDbName() + "." + tblMirror.getName() + " was SUCCESSFUL in " +
                                "previous run.   SKIPPING and adjusting status to RETRY_SKIPPED_PAST_SUCCESS");
                        tblMirror.setPhaseState(PhaseState.RETRY_SKIPPED_PAST_SUCCESS);
                        break;
                    case RETRY_SKIPPED_PAST_SUCCESS:
                        LOG.debug("DB.tbl: " + tblMirror.getDbName() + "." + tblMirror.getName() + " was SUCCESSFUL in " +
                                "previous run.  SKIPPING");
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

        Option metadataStage = new Option("m", "metastore", true,
                "Run HMS-Mirror Metadata with strategy: DIRECT(default)|EXPORT_IMPORT|SCHEMA_EXTRACT");
        metadataStage.setOptionalArg(Boolean.TRUE);
        metadataStage.setArgName("strategy");
        Option storageStage = new Option("s", "storage", true,
                "Run HMS-Mirror Storage with strategy: SQL|EXPORT_IMPORT|HYBRID(default)|DISTCP");
        storageStage.setArgName("strategy");
        storageStage.setOptionalArg(Boolean.TRUE);

        // WIP, not available yet.
//        Option convertStage = new Option("c", "convert", true,
//                "Run HMS-Mirror Storage with strategy: SQL|EXPORT_IMPORT");
//        convertStage.setArgName("strategy");
//        convertStage.setOptionalArg(Boolean.TRUE);

        OptionGroup stageGroup = new OptionGroup();
        stageGroup.addOption(metadataStage);
        stageGroup.addOption(storageStage);
//        stageGroup.addOption(convertStage);

        stageGroup.setRequired(true);
        options.addOptionGroup(stageGroup);

        Option helpOption = new Option("h", "help", false,
                "Help");
        helpOption.setRequired(false);
        options.addOption(helpOption);

        Option outputOption = new Option("f", "output-file", true,
                "Output Directory (default: $HOME/.hms-mirror/reports/hms-mirror-<stage>-<timestamp>.md");
        outputOption.setRequired(false);
        outputOption.setArgName("filename");
        options.addOption(outputOption);

        Option dryrunOption = new Option("dr", "dry-run", false,
                "No actions are performed, just the output of the commands in the logs.");
        dryrunOption.setRequired(false);
        options.addOption(dryrunOption);

        Option dbOption = new Option("db", "database", true,
                "Comma separated list of Databases (upto 100).");
        dbOption.setValueSeparator(',');
        dbOption.setArgName("databases");
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
        tableFilterOption.setArgName("regex");
        options.addOption(tableFilterOption);

        Option cfgOption = new Option("cfg", "config", true,
                "Config with details for the HMS-Mirror.  Default: $HOME/.hms-mirror/cfg/default.yaml");
        cfgOption.setRequired(false);
        cfgOption.setArgName("filename");
        options.addOption(cfgOption);

        Option retryOption = new Option("r", "retry", false,
                "Retry last incomplete run for 'cfg'.  If none specified, will check for 'default'");
        retryOption.setRequired(false);
        options.addOption(retryOption);

        return options;
    }

    public static void main(String[] args) {
        Mirror mirror = new Mirror();
        LOG.info("===================================================");
        LOG.info("Running: hms-mirror " + ReportingConf.substituteVariablesFromManifest("v.${Implementation-Version}"));
        LOG.info("===================================================");
        mirror.init(args);
        mirror.doit();
        System.exit(0);
    }
}
