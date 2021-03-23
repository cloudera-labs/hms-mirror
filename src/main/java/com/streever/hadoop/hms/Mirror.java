package com.streever.hadoop.hms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.stage.ReturnStatus;
import com.streever.hadoop.hms.stage.Setup;
import com.streever.hadoop.hms.stage.Transfer;
import com.streever.hadoop.hms.util.TableUtils;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.commonmark.Extension;
import org.commonmark.ext.front.matter.YamlFrontMatterExtension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.node.Node;
import org.commonmark.renderer.html.HtmlRenderer;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Mirror {
    private static Logger LOG = LogManager.getLogger(Mirror.class);

    private Config config = null;
    private String configFile = null;
    private String reportOutputDir = null;
    private String reportOutputFile = null;
    private String rightExecuteFile = null;
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

    protected void setupGSS() {
        try {
            String CURRENT_USER_PROP = "current.user";

            String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
            String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml"};

            // Get a value that over rides the default, if nothing then use default.
            String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

            // Set a default
            if (hadoopConfDirProp == null)
                hadoopConfDirProp = "/etc/hadoop/conf";

            Configuration hadoopConfig = new Configuration(true);

            File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
            for (String file : HADOOP_CONF_FILES) {
                File f = new File(hadoopConfDir, file);
                if (f.exists()) {
                    LOG.debug("Adding conf resource: '" + f.getAbsolutePath() + "'");
                    try {
                        // I found this new Path call failed on the Squadron Clusters.
                        // Not sure why.  Anyhow, the above seems to work the same.
                        hadoopConfig.addResource(new Path(f.getAbsolutePath()));
                    } catch (Throwable t) {
                        // This worked for the Squadron Cluster.
                        // I think it has something to do with the Docker images.
                        hadoopConfig.addResource("file:" + f.getAbsolutePath());
                    }
                }
            }
            // disable s3a fs cache
//                hadoopConfig.set("fs.s3a.impl.disable.cache", "true");
//                hadoopConfig.set("fs.s3a.bucket.probe","0");

            // hadoop.security.authentication
            if (hadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                try {
                    UserGroupInformation.setConfiguration(hadoopConfig);
                } catch (Throwable t) {
                    // Revert to non JNI. This happens in Squadron (Docker Imaged Hosts)
                    LOG.error("Failed GSS Init.  Attempting different Group Mapping");
                    hadoopConfig.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping");
                    UserGroupInformation.setConfiguration(hadoopConfig);
                }
            }
        } catch (Throwable t) {
            LOG.error("Issue initializing Kerberos", t);
            t.printStackTrace();
            throw t;
        }
    }

    public void init(String[] args) {

//        HadoopSession main = HadoopSession.get("MAIN");
//        String[] api = {"-api"};
//        try {
//            main.start(api);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        CommandReturn cr = main.processInput("connect");

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
            File defaultCfg = new File(configFile);
            if (!defaultCfg.exists()) {
                Config.setup(configFile);
            }
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

        if (cmd.hasOption("ma")) {
            config.setMigrateACID(Boolean.TRUE);
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

        if (cmd.hasOption("o")) {
            reportOutputDir = cmd.getOptionValue("o");
        } else {
            Date now = new Date();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            reportOutputDir = System.getProperty("user.home") + System.getProperty("file.separator") +
                    ".hms-mirror/reports/" + df.format(now);
        }

        // Action Files
//        int suffixIndex = reportOutputDir.lastIndexOf(".");
        reportOutputFile = reportOutputDir + System.getProperty("file.separator") + "<db>_hms-mirror.md|html";
        rightExecuteFile = reportOutputDir + System.getProperty("file.separator") + "<db>_execute.sql";
        leftActionFile = reportOutputDir + System.getProperty("file.separator") + "<db>_LEFT_action.sql";
        rightActionFile = reportOutputDir + System.getProperty("file.separator") + "<db>_RIGHT_action.sql";

        try {
//            String reportPath = reportOutputDir.substring(0, reportOutputDir.lastIndexOf(System.getProperty("file.separator")));
            File reportPathDir = new File(reportOutputDir);
            if (!reportPathDir.exists()) {
                reportPathDir.mkdirs();
            }
        } catch (StringIndexOutOfBoundsException stringIndexOutOfBoundsException) {
            // no dir in -f variable.
        }

        File testFile = new File(reportOutputDir + System.getProperty("file.separator") + ".dir-check");

        // Ensure the Retry Path is created.
        File retryPath = new File(System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror" +
                System.getProperty("file.separator") + "retry");
        if (!retryPath.exists()) {
            retryPath.mkdirs();
        }

        // Test file to ensure we can write to it for the report.
        try {
            new FileOutputStream(testFile).close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (cmd.hasOption("db")) {
            String[] databases = cmd.getOptionValues("db");
            if (databases != null)
                config.setDatabases(databases);
        }

        if (cmd.hasOption("sync")) {
            config.setSync(Boolean.TRUE);
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
                System.out.println("----------------------------------------");
                System.out.println(".... Accept/Acknowledge to continue ....");
                System.out.println("----------------------------------------");

                if (config.isSync() && !config.isReadOnly()) {
                    System.out.println("You have chosen to 'sync' WITHOUT 'Read-Only'");
                    System.out.println("\tWhich means there is a potential for DATA LOSS when out of sync tables are DROPPED and RECREATED.");
                    System.out.print("\tDo you accept this responsibility/scenario and the potential LOSS of DATA? (YES to proceed)");
                    String response = scanner.next();
                    if (!response.equalsIgnoreCase("yes")) {
                        throw new RuntimeException("You must accept to proceed.");
                    } else {
                        config.getAcceptance().setPotentialDataLoss(Boolean.TRUE);
                    }
                }

                // get their input as a String
                String response = scanner.next();
                if (!response.equalsIgnoreCase("true")) {
                    throw new RuntimeException("You must affirm to proceed.");
                } else {
                    config.getAcceptance().setBackedUpMetastore(Boolean.TRUE);
                }

                System.out.print("I have made backups of both the 'Hive Metastore' in the LEFT and RIGHT clusters (TRUE to proceed): ");

                // get their input as a String
                response = scanner.next();
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
        try {
            connPools.init();
        } catch (RuntimeException cnfe) {
            LOG.error("Issue initializing connections.  Check driver locations", cnfe);
            throw new RuntimeException(cnfe);
        }

        config.getCluster(Environment.LEFT).setPools(connPools);
        config.getCluster(Environment.RIGHT).setPools(connPools);

        if (config.isConnectionKerberized()) {
            LOG.debug("Detected a Kerberized JDBC Connection.  Attempting to setup/initialize GSS.");
            setupGSS();
        }
        LOG.debug("Checking Hive Connections");
        if (!config.checkConnections()) {
            LOG.error("Check Hive Connections Failed.");
            if (config.isConnectionKerberized()) {
                LOG.error("Check Kerberos configuration if GSS issues are encountered.  See the running.md docs for details.");
            }
            throw new RuntimeException("Check Hive Connections Failed.  Check Logs.");
        }

        if (!config.validate()) {
            List<String> issues = config.getIssues();
            for (String issue: issues) {
                LOG.error(issue);
            }
            throw new RuntimeException("Configuration issues, check log for details");
        }

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
        reporter.setVariable("execute.file", rightExecuteFile);
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

        for (String database : config.getDatabases()) {

            String dbReportOutputFile = reportOutputDir + System.getProperty("file.separator") + database + "_hms-mirror";
            String dbRightExecuteFile = reportOutputDir + System.getProperty("file.separator") + database + "_execute.sql";
            String dbLeftActionFile = reportOutputDir + System.getProperty("file.separator") + database + "_LEFT_action.sql";
            String dbRightActionFile = reportOutputDir + System.getProperty("file.separator") + database + "_RIGHT_action.sql";

            try {
                FileWriter reportFile = new FileWriter(dbReportOutputFile + ".md");
                String mdReportStr = conversion.toReport(config, database);
                reportFile.write(mdReportStr);
                reportFile.close();
                // Convert to HTML
                List<Extension> extensions = Arrays.asList(TablesExtension.create(), YamlFrontMatterExtension.create());

                org.commonmark.parser.Parser parser = org.commonmark.parser.Parser.builder().extensions(extensions).build();
                Node document = parser.parse(mdReportStr);
                HtmlRenderer renderer = HtmlRenderer.builder().extensions(extensions).build();
                String htmlReportStr = renderer.render(document);  // "<p>This is <em>Sparta</em></p>\n"
                reportFile = new FileWriter(dbReportOutputFile + ".html");
                reportFile.write(htmlReportStr);
                reportFile.close();

                LOG.info("Status Report of 'hms-mirror' is here: " + dbReportOutputFile.toString() + ".md|html");

                FileWriter rightExecOutput = new FileWriter(dbRightExecuteFile);
                rightExecOutput.write(conversion.executeSql(database));
                rightExecOutput.close();
                LOG.info("RIGHT Execution Script is here: " + dbRightExecuteFile.toString());

                FileWriter leftActionOutput = new FileWriter(dbLeftActionFile);
                leftActionOutput.write(conversion.actionsSql(Environment.LEFT, database));
                leftActionOutput.close();
                LOG.info("LEFT action SQL script is here: " + dbLeftActionFile.toString());

                FileWriter rightActionOutput = new FileWriter(dbRightActionFile);
                rightActionOutput.write(conversion.actionsSql(Environment.RIGHT, database));
                rightActionOutput.close();
                LOG.info("RIGHT action SQL script is here: " + dbRightActionFile.toString());
            } catch (IOException ioe) {
                LOG.error("Issue writing report for: " + database, ioe);
            }
        }
        Date endTime = new Date();
        DecimalFormat decf = new DecimalFormat("#.###");
        decf.setRoundingMode(RoundingMode.CEILING);
        LOG.info("HMS-Mirror: Completed in " +
                decf.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
        reporter.refresh();
        reporter.stop();
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
//            try {
//                config.getCluster(Environment.LEFT).getDatabase(config, dbMirror);
//                config.getCluster(Environment.RIGHT).getDatabase(config, dbMirror);
//            } catch (SQLException se) {
//                throw new RuntimeException(se);
//            }

            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                switch (tblMirror.getPhaseState()) {
                    case INIT:
                    case STARTED:
                    case ERROR:
                        Transfer md = new Transfer(config, dbMirror, tblMirror);
                        mdf.add(config.getTransferThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
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

        Option maOption = new Option("ma", "migrate-acid", false,
                "For EXPORT_IMPORT and HYBRID data strategies.  Include ACID tables in migration.");
        maOption.setRequired(false);
        options.addOption(maOption);

        Option syncOption = new Option("s", "sync", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies.  Drop and Recreate Schema's when different.  Best to use with RO to ensure table/partition drops don't delete data.");
        syncOption.setRequired(false);
        options.addOption(syncOption);

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

        Option outputOption = new Option("o", "output-dir", true,
                "Output Directory (default: $HOME/.hms-mirror/reports/<yyyy-MM-dd_HH-mm-ss>");
        outputOption.setRequired(false);
        outputOption.setArgName("outputdir");
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
            try {
                mirror.doit();
            } catch (RuntimeException rte) {
                System.out.println(rte.getMessage());
                rte.printStackTrace();
            }
            System.exit(0);
        } catch (RuntimeException e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            System.err.println("\nERROR: ==============================================");
            System.err.println(e.getMessage());
            System.err.println("\nSee log for stack trace");
            System.err.println("=====================================================");
            System.exit(-1);
        }
    }
}
