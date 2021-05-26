package com.streever.hadoop.hms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.mirror.*;
import com.streever.hadoop.hms.mirror.feature.Features;
import com.streever.hadoop.hms.stage.ReturnStatus;
import com.streever.hadoop.hms.stage.Setup;
import com.streever.hadoop.hms.stage.Transfer;
import com.streever.hadoop.hms.util.Protect;
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
import java.util.concurrent.Future;
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
        if (cmd.hasOption("su")) {
            configFile = System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/cfg/default.yaml";
            File defaultCfg = new File(configFile);
            if (defaultCfg.exists()) {
                Scanner scanner = new Scanner(System.in);
                System.out.print("Default Config exists.  Proceed with overwrite:(Y/N) ");
                String response = scanner.next();
                if (response.equalsIgnoreCase("y")) {
                    Config.setup(configFile);
                    System.exit(0);
                }
            } else {
                Config.setup(configFile);
                System.exit(0);
            }
        }

        if (cmd.hasOption("p")) {
            // Used to generate encrypted password.
            if (cmd.hasOption("pkey")) {
                Protect protect = new Protect(cmd.getOptionValue("pkey"));
                String epassword = null;
                try {
                    epassword = protect.encrypt(cmd.getOptionValue("p"));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
                System.out.println("Encrypted password: " + epassword);
            } else {
                System.err.println("Need to include '-pkey' with '-p'.");
                System.exit(-1);
            }

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
                System.exit(0);
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

        if (cmd.hasOption("t")) {
            Translator translator = null;
            File tCfgFile = new File(cmd.getOptionValue("t"));
            if (!tCfgFile.exists()) {
                throw new RuntimeException("Couldn't locate translation configuration file: " + cmd.getOptionValue("t"));
            } else {
                try {
                    System.out.println("Using Translation Config: " + cmd.getOptionValue("t"));
                    String yamlCfgFile = FileUtils.readFileToString(tCfgFile, Charset.forName("UTF-8"));
                    translator = mapper.readerFor(Translator.class).readValue(yamlCfgFile);
                    if (translator.validate()) {
                        config.setTranslator(translator);
                    } else {
                        throw new RuntimeException("Translator config can't be validated, check logs.");
                    }
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }

            }
        }

        // When the pkey is specified, we assume the config passwords are encrytped and we'll decrypt them before continuing.
        if (cmd.hasOption("pkey")) {
            // Loop through the HiveServer2 Configs and decode the password.
            System.out.println("Password Key specified.  Decrypting config password before submitting.");

            String pkey = cmd.getOptionValue("pkey");
            Protect protect = new Protect(pkey);

            for (Environment env : Environment.values()) {
                Cluster cluster = config.getCluster(env);
                if (cluster != null) {
                    HiveServer2Config hiveServer2Config = cluster.getHiveServer2();
                    Properties props = hiveServer2Config.getConnectionProperties();
                    String password = props.getProperty("password");
                    if (password != null) {
                        try {
                            String decryptedPassword = protect.decrypt(password);
                            props.put("password", decryptedPassword);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println("Issue decrypting password");
                            System.exit(-1);
                        }
                    }
                }
            }
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
                if (config.getDataStrategy() == DataStrategy.DUMP) {
                    config.setExecute(Boolean.FALSE); // No Actions.
                    config.setSync(Boolean.FALSE);
                }
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

        if (cmd.hasOption("f")) {
            String[] featuresStr = cmd.getOptionValues("f");
            List<Features> featuresList = new ArrayList<Features>();
//            Feature[] features = new Feature[featuresStr.length];
            // convert feature string to feature enum.q
            for (String featureStr : featuresStr) {
                try {
                    Features feature = Features.valueOf(featureStr);
                    featuresList.add(feature);
                } catch (IllegalArgumentException iae) {
                    if (featureStr.equalsIgnoreCase("TRANSLATE")) {
                        config.getTranslator().setOn(Boolean.TRUE);
                    } else {
                        throw new RuntimeException(featureStr + " is NOT a valid 'feature'. One or more of: " + Arrays.deepToString(Features.values()) + ",TRANSLATE");
                    }
                }

            }
            if (featuresList.size() > 0) {
                Features[] features = featuresList.toArray(new Features[0]);
                config.setFeatures(features);
            }
        }

        if (cmd.hasOption("sync") && config.getDataStrategy() != DataStrategy.DUMP) {
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

        if (cmd.hasOption("e") && config.getDataStrategy() != DataStrategy.DUMP) {
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
        switch (config.getDataStrategy()) {
            case DUMP:
                // Don't load the datasource for the right with DUMP strategy.
                break;
            default:
                connPools.addHiveServer2(Environment.RIGHT, config.getCluster(Environment.RIGHT).getHiveServer2());
        }
        try {
            connPools.init();
        } catch (RuntimeException cnfe) {
            LOG.error("Issue initializing connections.  Check driver locations", cnfe);
            throw new RuntimeException(cnfe);
        }

        config.getCluster(Environment.LEFT).setPools(connPools);
        switch (config.getDataStrategy()) {
            case DUMP:
                // Don't load the datasource for the right with DUMP strategy.
                break;
            default:
                config.getCluster(Environment.RIGHT).setPools(connPools);
        }

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
            for (String issue : issues) {
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
        // Output directory maps
        try {
            String distcpWorkbookFile = reportOutputDir + System.getProperty("file.separator") + "distcp_workbook.md";
            FileWriter distcpWorkbookFW = new FileWriter(distcpWorkbookFile);

            distcpWorkbookFW.write("| Database | Target | Sources |\n");
            distcpWorkbookFW.write("|:---|:---|:---|\n");


            for (Map.Entry<String, Map<String, Set<String>>> entry :
                    config.getTranslator().buildDistcpList(1).entrySet()) {
                distcpWorkbookFW.write("| " + entry.getKey() + " | | |\n");
                Map<String, Set<String>> value = entry.getValue();
                for (Map.Entry<String, Set<String>> dbMap : value.entrySet()) {
                    StringBuilder line = new StringBuilder();
                    line.append("| | ").append(dbMap.getKey()).append(" | ");

                    for (String source : dbMap.getValue()) {
                        line.append(source).append("<br>");
                    }
                    line.append(" | ").append("\n");
                    distcpWorkbookFW.write(line.toString());
                }
            }

            distcpWorkbookFW.close();
        } catch (IOException ioe) {
            LOG.error("Issue writing distcp workbook", ioe);
        }

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
        List<Future<ReturnStatus>> mdf = new ArrayList<Future<ReturnStatus>>();

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
//                        mdf.add(config.getTransferThreadPool().submit(md));
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
            for (Future<ReturnStatus> sf : mdf) {
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
        metadataStage.setRequired(Boolean.FALSE);
        options.addOption(metadataStage);

        Option intermediateStorageOption = new Option("is", "intermediate-storage", true,
                "Intermediate Storage used with Data Strategy INTERMEDIATE.");
        intermediateStorageOption.setOptionalArg(Boolean.TRUE);
        intermediateStorageOption.setArgName("storage-path");
        intermediateStorageOption.setRequired(Boolean.FALSE);
        options.addOption(intermediateStorageOption);

        Option maOption = new Option("ma", "migrate-acid", false,
                "For EXPORT_IMPORT and HYBRID data strategies.  Include ACID tables in migration.");
        maOption.setRequired(Boolean.FALSE);
        options.addOption(maOption);

        Option syncOption = new Option("s", "sync", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies.  Drop and Recreate Schema's when different.  Best to use with RO to ensure table/partition drops don't delete data.");
        syncOption.setRequired(Boolean.FALSE);
        options.addOption(syncOption);

        Option roOption = new Option("ro", "read-only", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP");
        roOption.setRequired(Boolean.FALSE);
        options.addOption(roOption);

        Option acceptOption = new Option("accept", "accept", false,
                "Accept ALL confirmations and silence prompts");
        acceptOption.setRequired(Boolean.FALSE);
        options.addOption(acceptOption);

        Option translateConfigOption = new Option("t", "translate-config", true,
                "Translator Configuration File (Experimental)");
        translateConfigOption.setRequired(Boolean.FALSE);
        translateConfigOption.setArgName("translate-config-file");
        options.addOption(translateConfigOption);


        Option featureOption = new Option("f", "feature", true,
                "Added Feature(s) Checks: " + Arrays.deepToString(Features.values()));
        // Let's not advertise the TRANSLATE feature yet.
//                "Added Feature Checks[BAD_ORC_DEF,TRANSLATE(WIP)]");
        featureOption.setValueSeparator(',');
        featureOption.setArgName("features (comma-separated)");
        featureOption.setArgs(20);
        featureOption.setRequired(Boolean.FALSE);
        options.addOption(featureOption);

        Option outputOption = new Option("o", "output-dir", true,
                "Output Directory (default: $HOME/.hms-mirror/reports/<yyyy-MM-dd_HH-mm-ss>");
        outputOption.setRequired(Boolean.FALSE);
        outputOption.setArgName("outputdir");
        options.addOption(outputOption);

        Option executeOption = new Option("e", "execute", false,
                "Execute actions request, without this flag the process is a dry-run.");
        executeOption.setRequired(Boolean.FALSE);
        options.addOption(executeOption);

        Option dbPrefixOption = new Option("dbp", "db-prefix", true,
                "Optional: A prefix to add to the RIGHT cluster DB Name. Usually used for testing.");
        dbPrefixOption.setRequired(Boolean.FALSE);
        dbPrefixOption.setArgName("prefix");
        options.addOption(dbPrefixOption);

        Option dbOption = new Option("db", "database", true,
                "Comma separated list of Databases (upto 100).");
        dbOption.setValueSeparator(',');
        dbOption.setArgName("databases");
        dbOption.setArgs(100);

        Option helpOption = new Option("h", "help", false,
                "Help");
        helpOption.setRequired(Boolean.FALSE);
//        options.addOption(helpOption);

        Option pwOption = new Option("p", "password", true,
                "Used this in conjunction with '-pkey' to generate the encrypted password that you'll add to the configs for the JDBC connections.");
        pwOption.setRequired(Boolean.FALSE);
        pwOption.setArgName("password");
        options.addOption(pwOption);

        Option setupOption = new Option("su", "setup", false,
                "Setup a default configuration file through a series of questions");
        setupOption.setRequired(Boolean.FALSE);
//        options.addOption(setupOption);

        OptionGroup dbGroup = new OptionGroup();
        dbGroup.addOption(dbOption);
        dbGroup.addOption(helpOption);
        dbGroup.addOption(setupOption);
        dbGroup.setRequired(Boolean.TRUE);
        options.addOptionGroup(dbGroup);

        Option sqlOutputOption = new Option("sql", "sql-output", false,
                "Output the SQL to the report");
        sqlOutputOption.setRequired(Boolean.FALSE);
        options.addOption(sqlOutputOption);

        Option tableFilterOption = new Option("tf", "table-filter", true, "Filter tables with name matching RegEx. Comparison done with 'show tables' results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE.");
        tableFilterOption.setRequired(Boolean.FALSE);
        tableFilterOption.setArgName("regex");
        options.addOption(tableFilterOption);

        Option cfgOption = new Option("cfg", "config", true,
                "Config with details for the HMS-Mirror.  Default: $HOME/.hms-mirror/cfg/default.yaml");
        cfgOption.setRequired(false);
        cfgOption.setArgName("filename");
        options.addOption(cfgOption);

        Option pKeyOption = new Option("pkey", "password-key", true,
                "The key used to encrypt / decrypt the cluster jdbc passwords.  If not present, the passwords will be processed as is (clear text) from the config file.");
        pKeyOption.setRequired(false);
        pKeyOption.setArgName("password-key");
        options.addOption(pKeyOption);


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
