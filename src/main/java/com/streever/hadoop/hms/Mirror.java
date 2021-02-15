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
    private String leftActionFile = null;
    private String rightActionFile = null;
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
            throw new RuntimeException(pe);
        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror \nversion:${Implementation-Version}");
            formatter.printHelp(cmdline, options);
            System.exit(0);
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
            throw new RuntimeException(t);
        }

        if (cmd.hasOption("dbp")) {
            config.setDbPrefix(cmd.getOptionValue("dbp"));
        }

        String dataStrategyStr = cmd.getOptionValue("d");
        // default is SCHEMA_ONLY
        if (dataStrategyStr != null) {
            try {
                DataStrategy dataStrategy = DataStrategy.valueOf(dataStrategyStr.toUpperCase());
                config.setDataStrategy(dataStrategy);
            } catch (Exception e) {
                throw new RuntimeException("Can't translate " + dataStrategyStr + " to a known 'data strategy'.  Use one of " + Arrays.deepToString(DataStrategy.values()));
            }
        }

        if (config.getDataStrategy() == DataStrategy.INTERMEDIATE) {
            // Get intermediate Storage Location
            if (cmd.hasOption("is")) {
                config.getTransfer().setIntermediateStorage(cmd.getOptionValue("is"));
            } else {
                throw new RuntimeException("Need to specify '-is/--itermediate-storage' when using INTERMEDIATE data strategy.");
            }
        }

        if (cmd.hasOption("ro")) {
            switch (config.getDataStrategy()) {
                case SCHEMA_ONLY:
                case LINKED:
                case COMMON:
                    config.setReadOnly(Boolean.TRUE);
                    break;
                default:
                    throw new RuntimeException("RO option only valid with SCHEMA_ONLY,COMMON, and LINKED data strategies.");
            }
        }

        if (cmd.hasOption("sql")) {
            config.setSqlOutput(Boolean.TRUE);
        }

        if (cmd.hasOption("r")) {
            retry = Boolean.TRUE;
        }

        if (cmd.hasOption("f")) {
            reportOutputFile = cmd.getOptionValue("f");
        } else {
            reportOutputFile = System.getProperty("user.home") + System.getProperty("file.separator") +
                    ".hms-mirror/reports/hms-mirror-" + getDateMarker() + ".md";
        }

        // Action Files
        int suffixIndex = reportOutputFile.lastIndexOf(".");
        leftActionFile = reportOutputFile.substring(0, suffixIndex) + "_LEFT_action.sql";
        rightActionFile = reportOutputFile.substring(0, suffixIndex) + "_RIGHT_action.sql";

        try {
            String reportPath = reportOutputFile.substring(0, reportOutputFile.lastIndexOf(System.getProperty("file.separator")));
            File reportPathDir = new File(reportPath);
            if (!reportPathDir.exists()) {
                reportPathDir.mkdirs();
            }
        } catch (StringIndexOutOfBoundsException stringIndexOutOfBoundsException) {
            // no dir in -f variable.
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
            throw new RuntimeException(e);
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
            throw new RuntimeException("No databases specified");
        }

        if (cmd.hasOption("e")) {
            if (cmd.hasOption("accept")) {
                config.getAcceptance().setSilentOverride(Boolean.TRUE);
            } else {
                Scanner scanner = new Scanner(System.in);

                //  prompt for the user's name
                System.out.println("----------------------------");
                System.out.println(".... Accept to continue ....");
                System.out.println("----------------------------");
                System.out.print("I have made backups of both the 'Hive Metastore' in the LEFT and RIGHT clusters (TRUE to proceed): ");

                // get their input as a String
                String response = scanner.next();
                if (!response.equalsIgnoreCase("true")) {
                    throw new RuntimeException("You must affirm to proceed.");
                } else {
                    config.getAcceptance().setBackedUpMetastore(Boolean.TRUE);
                }
                System.out.print("I have taken 'Filesystem' Snapshots/Backups of the target 'Hive Databases' on the LEFT and RIGHT clusters (TRUE to proceed): ");
                response = scanner.next();
                if (!response.equalsIgnoreCase("true")) {
                    throw new RuntimeException("You must affirm to proceed.");
                } else {
                    config.getAcceptance().setBackedUpHDFS(Boolean.TRUE);
                }

                System.out.print("'Filesystem' TRASH has been configured on my system (TRUE to proceed): ");
                response = scanner.next();
                if (!response.equalsIgnoreCase("true")) {
                    throw new RuntimeException("You must affirm to proceed.");
                } else {
                    config.getAcceptance().setTrashConfigured(Boolean.TRUE);
                }
            }
            config.setExecute(Boolean.TRUE);
        } else {
            LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            LOG.info("EXECUTE has NOT been set.  No ACTIONS will be performed, the process output will be recorded in the log.");
            LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            config.setExecute(Boolean.FALSE);
        }

        ConnectionPools connPools = new ConnectionPools();
        connPools.addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
        connPools.addHiveServer2(Environment.RIGHT, config.getCluster(Environment.RIGHT).getHiveServer2());
        connPools.init();

        config.getCluster(Environment.LEFT).setPools(connPools);
        config.getCluster(Environment.RIGHT).setPools(connPools);

    }

    public void doit() {
        Conversion conversion = new Conversion(config);

        // Setup and Start the State Maintenance Routine
        StateMaintenance stateMaintenance = new StateMaintenance(5000, configFile, getDateMarker());

        if (retry) {
            File retryFile = stateMaintenance.getRetryFile();
            if (!retryFile.exists()) {
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
                throw new RuntimeException("Could NOT read 'retry' file: " + retryFile.getPath(), e);
            }
        }

        // Link the conversion to the state machine.
        stateMaintenance.setConversion(conversion);

        // Setup and Start the Reporter
        Reporter reporter = new Reporter(conversion, 1000);
        reporter.setVariable("config.file", configFile);
        reporter.setVariable("config.strategy", config.getDataStrategy().toString());
//        reporter.setVariable("stage", config.getStage().toString());
        // ?
        //        reporter.setVariable("log.file", log4j output file.);
        reporter.setVariable("report.file", reportOutputFile);
        reporter.setVariable("left.action.file", leftActionFile);
        reporter.setVariable("right.action.file", rightActionFile);
        reporter.setRetry(this.retry);
        reporter.start();

        Date startTime = new Date();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

        if (config.isExecute()) {
            reporter.setVariable("run.mode", "EXECUTE");
        } else {
            reporter.setVariable("run.mode", "DRY-RUN");
        }

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

        conversion = runTransfer(conversion);
        stateMaintenance.saveState();

        // Actions

        try {
            FileWriter reportFile = new FileWriter(reportOutputFile);
            reportFile.write(conversion.toReport(config));

            // When this happens, we need to output the sql to 'detach' the
            //    lower cluster tables from the data WHEN shared storage is used.
//            if (config.isCommitToUpper() && config.isShareStorage()) {
                // TODO:
//            }

            reportFile.close();
            LOG.info("Status Report of 'hms-mirror' is here: " + reportOutputFile.toString());

            FileWriter leftActionOutput = new FileWriter(leftActionFile);
            leftActionOutput.write(conversion.actionsSql(Environment.LEFT));
            leftActionOutput.close();
            LOG.info("LEFT action SQL script is here: " + leftActionFile.toString());

            FileWriter rightActionOutput = new FileWriter(rightActionFile);
            rightActionOutput.write(conversion.actionsSql(Environment.RIGHT));
            rightActionOutput.close();
            LOG.info("RIGHT action SQL script is here: " + rightActionFile.toString());

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

    public Conversion runTransfer(Conversion conversion) {
        Date startTime = new Date();
        LOG.info("Start Processing for databases: " + Arrays.toString((config.getDatabases())));
//        Conversion conversion = new Conversion();

        LOG.info(">>>>>>>>>>> Building/Starting Transition.");
        List<ScheduledFuture<ReturnStatus>> mdf = new ArrayList<ScheduledFuture<ReturnStatus>>();

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
                        if (!TableUtils.isACID(tblMirror.getName(), tblMirror.getTableDefinition(Environment.LEFT))) {
                            Transfer md = new Transfer(config, dbMirror, tblMirror);
                            mdf.add(config.getTransferThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
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
            for (ScheduledFuture<ReturnStatus> sf : mdf) {
                if (!sf.isDone()) {
                    check = false;
                    break;
                }
                try {
                    if (sf.isDone() && sf.get() != null) {
                        if (sf.get().getStatus() == ReturnStatus.Status.ERROR) {
                            throw new RuntimeException(sf.get().getException());
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            if (check)
                break;
        }

        config.getTransferThreadPool().shutdown();

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

        Option metadataStage = new Option("d", "data", true,
                "Specify how the data will follow the schema. " + Arrays.deepToString(DataStrategy.values()));
        metadataStage.setOptionalArg(Boolean.TRUE);
        metadataStage.setArgName("strategy");
        metadataStage.setRequired(false);
        options.addOption(metadataStage);

        Option intermediateStorageOption = new Option("is", "intermediate-storage", true,
        "Intermediate Storage used with Data Strategy INTERMEDIATE.");
        intermediateStorageOption.setOptionalArg(Boolean.TRUE);
        intermediateStorageOption.setArgName("storage-path");
        intermediateStorageOption.setRequired(Boolean.FALSE);
        options.addOption(intermediateStorageOption);

        Option roOption = new Option("ro", "read-only", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP");
        roOption.setRequired(false);
        options.addOption(roOption);

        Option acceptOption = new Option("accept", "accept", false,
                "Accept ALL confirmations and silence prompts");
        acceptOption.setRequired(false);
        options.addOption(acceptOption);

        Option helpOption = new Option("h", "help", false,
                "Help");
        helpOption.setRequired(false);
        options.addOption(helpOption);

        Option outputOption = new Option("f", "output-file", true,
                "Output Directory (default: $HOME/.hms-mirror/reports/hms-mirror-<timestamp>.md");
        outputOption.setRequired(false);
        outputOption.setArgName("filename");
        options.addOption(outputOption);

        Option executeOption = new Option("e", "execute", false,
                "Execute actions request, without this flag the process is a dry-run.");
        executeOption.setRequired(false);
        options.addOption(executeOption);

        Option dbPrefixOption = new Option("dbp", "db-prefix", true,
                "Optional: A prefix to add to the RIGHT cluster DB Name. Usually used for testing.");
        dbPrefixOption.setRequired(false);
        dbPrefixOption.setArgName("prefix");
        options.addOption(dbPrefixOption);

        Option dbOption = new Option("db", "database", true,
                "Comma separated list of Databases (upto 100).");
        dbOption.setValueSeparator(',');
        dbOption.setArgName("databases");
        dbOption.setArgs(100);
        OptionGroup dbGroup = new OptionGroup();
        dbGroup.addOption(dbOption);
        dbGroup.setRequired(true);
        options.addOptionGroup(dbGroup);

        Option sqlOutputOption = new Option("sql", "sql-output", false,
        "Output the SQL to the report");
        sqlOutputOption.setRequired(Boolean.FALSE);
        options.addOption(sqlOutputOption);

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
        try {
            mirror.init(args);
            mirror.doit();
            System.exit(0);
        } catch (RuntimeException e) {
            LOG.error(e);
            System.err.println("\nERROR: ==============================================");
            System.err.println(e.getMessage());
            System.err.println("\nSee log for stack trace");
            System.err.println("=====================================================");
            System.exit(-1);
        }
    }
}
