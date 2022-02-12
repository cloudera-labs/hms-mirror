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

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.stage.ReturnStatus;
import com.cloudera.utils.hadoop.hms.stage.Setup;
import com.cloudera.utils.hadoop.hms.stage.Transfer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Sets;
import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.util.Protect;
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
import java.util.concurrent.TimeUnit;

public class Mirror {
    private static Logger LOG = LogManager.getLogger(Mirror.class);

    private Config config = null;
    private String configFile = null;
    private String reportOutputDir = null;
    private String reportOutputFile = null;
    private String leftExecuteFile = null;
    private String rightExecuteFile = null;
    private String leftActionFile = null;
    private String rightActionFile = null;
    private Boolean retry = Boolean.FALSE;
    private Boolean quiet = Boolean.FALSE;
    private String dateMarker;

    public Boolean getQuiet() {
        return quiet;
    }

    public void setQuiet(Boolean quiet) {
        this.quiet = quiet;
    }

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
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror <options> \nversion:${Implementation-Version}");
            formatter.printHelp(100, cmdline, "Hive Metastore Migration Utility", options,
                    "\nVisit https://github.com/dstreev/hms-mirror/blob/main/README.md for detailed docs.");
//            formatter.printHelp(cmdline, options);
            throw new RuntimeException(pe);
        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror <options> \nversion:${Implementation-Version}");
            formatter.printHelp(100, cmdline, "Hive Metastore Migration Utility", options,
                    "\nVisit https://github.com/dstreev/hms-mirror/blob/main/README.md for detailed docs");
//            formatter.printHelp(cmdline, options);
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
            // Look for yaml update errors.
            if (t.toString().contains("MismatchedInputException")) {
                throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                        "'-su|--setup' again to recreate in the new format", t);
            } else {
                System.err.println("======");
                System.err.println("A configuration element is no longer valid (progress!!!).  Please remove the element from the configuration 'yaml' and try again.");
                System.err.println(t.getMessage());
                System.err.println("======");
                LOG.error(t);
                throw new RuntimeException("Configuration invalid.  Review message and make adjustments.");
            }
        }

        if (cmd.hasOption("sf")) {
            // Skip Features.
            config.setSkipFeatures(Boolean.TRUE);
        }

        if (cmd.hasOption("q")) {
            // Skip Features.
            this.setQuiet(Boolean.TRUE);
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

        if (cmd.hasOption("v")) {
            config.getMigrateVIEW().setOn(Boolean.TRUE);
        }

        if (cmd.hasOption("dbo")) {
            config.setDatabaseOnly(Boolean.TRUE);
        }

        if (cmd.hasOption("ma")) {
            config.getMigrateACID().setOn(Boolean.TRUE);
            String bucketLimit = cmd.getOptionValue("ma");
            if (bucketLimit != null) {
                config.getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
            }
        }

        if (cmd.hasOption("mao")) {
            config.getMigrateACID().setOnly(Boolean.TRUE);
            String bucketLimit = cmd.getOptionValue("mao");
            if (bucketLimit != null) {
                config.getMigrateACID().setArtificialBucketThreshold(Integer.valueOf(bucketLimit));
            }
        }

        if (config.getMigrateACID().isOn()) {
            if (cmd.hasOption("da")) {
                // Downgrade ACID tables
                config.getMigrateACID().setDowngrade(Boolean.TRUE);
            }
        }

        if (cmd.hasOption("mnn")) {
            config.setMigratedNonNative(Boolean.TRUE);
        }

        if (cmd.hasOption("mnno")) {
            config.setMigratedNonNative(Boolean.TRUE);
        }

        // AVRO Schema Migration
        if (cmd.hasOption("asm")) {
            config.setCopyAvroSchemaUrls(Boolean.TRUE);
        }

        String dataStrategyStr = cmd.getOptionValue("d");
        // default is SCHEMA_ONLY
        if (dataStrategyStr != null) {
            DataStrategy dataStrategy = DataStrategy.valueOf(dataStrategyStr.toUpperCase());
            config.setDataStrategy(dataStrategy);
            if (config.getDataStrategy() == DataStrategy.DUMP) {
                config.setExecute(Boolean.FALSE); // No Actions.
                config.setSync(Boolean.FALSE);
                // If a source cluster is specified for the cluster to DUMP from, set it.
                if (cmd.hasOption("ds")) {
                    try {
                        Environment source = Environment.valueOf(cmd.getOptionValue("ds").toUpperCase());
                        config.setDumpSource(source);
                    } catch (RuntimeException re) {
                        LOG.error("The `-ds` option should be either: (LEFT|RIGHT). " + cmd.getOptionValue("ds") +
                                " is NOT a valid option.");
                        throw new RuntimeException("The `-ds` option should be either: (LEFT|RIGHT). " + cmd.getOptionValue("ds") +
                                " is NOT a valid option.");
                    }
                } else {
                    config.setDumpSource(Environment.LEFT);
                }
            }
            if (config.getDataStrategy() == DataStrategy.LINKED) {
                if (cmd.hasOption("ma") || cmd.hasOption("mao")) {
                    LOG.error("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
                    throw new RuntimeException("Can't LINK ACID tables.  ma|mao options are not valid with LINKED data strategy.");
                }
            }
        }

        // To keep the connections and remainder of the processing in place, set the env for the cluster
        //   to the abstract name.
        Set<Environment> environmentSet = Sets.newHashSet(Environment.LEFT, Environment.RIGHT);
        for (Environment lenv : environmentSet) {
            config.getCluster(lenv).setEnvironment(lenv);
        }

        // Get intermediate Storage Location
        if (cmd.hasOption("is")) {
            config.getTransfer().setIntermediateStorage(cmd.getOptionValue("is"));
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

//        if (cmd.hasOption("r")) {
//            retry = Boolean.TRUE;
//        }

        if (cmd.hasOption("o")) {
            reportOutputDir = cmd.getOptionValue("o");
        } else {
            Date now = new Date();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            reportOutputDir = System.getProperty("user.home") + System.getProperty("file.separator") +
                    ".hms-mirror/reports/" + df.format(now);
        }

        // Action Files
        reportOutputFile = reportOutputDir + System.getProperty("file.separator") + "<db>_hms-mirror.md|html";
        leftExecuteFile = reportOutputDir + System.getProperty("file.separator") + "<db>_LEFT_execute.sql";
        rightExecuteFile = reportOutputDir + System.getProperty("file.separator") + "<db>_RIGHT_execute.sql";
        leftActionFile = reportOutputDir + System.getProperty("file.separator") + "<db>_LEFT_action.sql";
        rightActionFile = reportOutputDir + System.getProperty("file.separator") + "<db>_RIGHT_action.sql";

        try {
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

        if (!config.validate()) {
            List<String> issues = config.getIssues();
            System.err.println("");
            for (String issue : issues) {
                LOG.error(issue);
                System.err.println(issue);
            }
            System.err.println("");
            throw new RuntimeException("Configuration issues., check log (~/.hms-mirror/logs/hms-mirror.log) for details");
        }

        ConnectionPools connPools = new ConnectionPools();
        switch (config.getDataStrategy()) {
            case DUMP:
                // Don't load the datasource for the right with DUMP strategy.
                if (config.getDumpSource() == Environment.RIGHT) {
                    // switch LEFT and RIGHT
                    config.getClusters().remove(Environment.LEFT);
                    config.getClusters().put(Environment.LEFT, config.getCluster(Environment.RIGHT));
                    config.getCluster(Environment.LEFT).setEnvironment(Environment.LEFT);
                    config.getClusters().remove(Environment.RIGHT);
                }
                // Get Pool
                connPools.addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
                break;
            default:
                connPools.addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
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
    }

    public void doit() {
        Conversion conversion = new Conversion(config);

        // Setup and Start the State Maintenance Routine
        StateMaintenance stateMaintenance = new StateMaintenance(10000, configFile, getDateMarker());

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
        reporter.setQuiet(getQuiet());
        reporter.setVariable("config.file", configFile);
        reporter.setVariable("config.strategy", config.getDataStrategy().toString());
        reporter.setVariable("report.file", reportOutputFile);
        reporter.setVariable("left.execute.file", leftExecuteFile);
        reporter.setVariable("right.execute.file", rightExecuteFile);
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
            // This will collect all existing DB/Table Definitions in the clusters
            Setup setup = new Setup(config, conversion);
            // TODO: Failure here may not make it to saved state.
            if (setup.collect()) {
//                stateMaintenance.saveState();
            } else {
                // Need to delete retry file.
//                stateMaintenance.deleteState();
            }
        }

//        stateMaintenance.start();
        // State reason table/view was removed from processing list.
        for (Map.Entry<String, DBMirror> dbEntry : conversion.getDatabases().entrySet()) {
            if (config.getDatabaseOnly()) {
                dbEntry.getValue().addIssue("FYI:Only processing DB.");
            }
            for (TableMirror tm : dbEntry.getValue().getTableMirrors().values()) {
                if (tm.isRemove()) {
                    dbEntry.getValue().getFilteredOut().put(tm.getName(), tm.getRemoveReason());
                }
            }
        }

        // Remove all the tblMirrors that shouldn't be processed based on config.
        for (Map.Entry<String, DBMirror> dbEntry : conversion.getDatabases().entrySet()) {
            dbEntry.getValue().getTableMirrors().values().removeIf(value -> value.isRemove());
        }

        // GO TIME!!!
        conversion = runTransfer(conversion);
//        stateMaintenance.saveState();
        // Actions

        // Remove the abstract environments from config before reporting output.
        config.getClusters().remove(Environment.TRANSFER);
        config.getClusters().remove(Environment.SHADOW);

        for (String database : config.getDatabases()) {

            String dbReportOutputFile = reportOutputDir + System.getProperty("file.separator") + database + "_hms-mirror";
            String dbLeftExecuteFile = reportOutputDir + System.getProperty("file.separator") + database + "_LEFT_execute.sql";
            String dbRightExecuteFile = reportOutputDir + System.getProperty("file.separator") + database + "_RIGHT_execute.sql";
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

                FileWriter leftExecOutput = new FileWriter(dbLeftExecuteFile);
                leftExecOutput.write(conversion.executeSql(Environment.LEFT, database));
                leftExecOutput.close();
                LOG.info("LEFT Execution Script is here: " + dbLeftExecuteFile.toString());

                FileWriter rightExecOutput = new FileWriter(dbRightExecuteFile);
                rightExecOutput.write(conversion.executeSql(Environment.RIGHT, database));
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
            String distcpScriptFile = reportOutputDir + System.getProperty("file.separator") + "distcp_script.sh";
            FileWriter distcpScriptFW = new FileWriter(distcpScriptFile);

            distcpScriptFW.append("\n");
            distcpScriptFW.append("# 1. Copy the source '*_distcp_source.txt' files to the distributed filesystem.").append("\n");
            distcpScriptFW.append("# 2. Export an env var 'HCFS_BASE_DIR' that represents where these files where placed.").append("\n");
            distcpScriptFW.append("#      NOTE: ${HCFS_BASE_DIR} must be available to the user running 'distcp'").append("\n");
            distcpScriptFW.append("# 3. Export an env var 'DISTCP_OPTS' with any special settings needed to run the job.").append("\n");
            distcpScriptFW.append("#      For large jobs, you may need to adjust memory settings.").append("\n");
            distcpScriptFW.append("# 4. Run the following in an order or framework that is appropriate for your environment.").append("\n");
            distcpScriptFW.append("#       These aren't necessarily expected to run in this shell script as is in production.").append("\n");
            distcpScriptFW.append("\n");
            distcpScriptFW.append("\n");


            distcpWorkbookFW.write("| Database | Target | Sources |\n");
            distcpWorkbookFW.write("|:---|:---|:---|\n");

            FileWriter distcpSourceFW = null;
            for (Map.Entry<String, Map<String, Set<String>>> entry :
                    config.getTranslator().buildDistcpList(1).entrySet()) {

                distcpWorkbookFW.write("| " + entry.getKey() + " | | |\n");
                Map<String, Set<String>> value = entry.getValue();
                int i = 1;
                for (Map.Entry<String, Set<String>> dbMap : value.entrySet()) {
                    String distcpSourceFile = entry.getKey() + "_" + i++ + "_distcp_source.txt";
                    String distcpSourceFileFull = reportOutputDir + System.getProperty("file.separator") + distcpSourceFile;
                    distcpSourceFW = new FileWriter(distcpSourceFileFull);

                    StringBuilder line = new StringBuilder();
                    line.append("| | ").append(dbMap.getKey()).append(" | ");

                    for (String source : dbMap.getValue()) {
                        line.append(source).append("<br>");
                        distcpSourceFW.append(source).append("\n");
                    }
                    line.append(" | ").append("\n");
                    distcpWorkbookFW.write(line.toString());
                    distcpScriptFW.append("hadoop distcp ${DISTCP_OPTS} -f ${HCFS_BASE_DIR}/" + distcpSourceFile + " " +
                            dbMap.getKey() + "\n");
                    distcpSourceFW.close();
                }
            }
            distcpScriptFW.close();
            distcpWorkbookFW.close();
        } catch (IOException ioe) {
            LOG.error("Issue writing distcp workbook", ioe);
        }

        LOG.info("HMS-Mirror: Completed in " +
                decf.format((Double) ((endTime.getTime() - startTime.getTime()) / (double) 1000)) + " secs");
        reporter.refresh(Boolean.TRUE);
        reporter.stop();
    }

    public Conversion runTransfer(Conversion conversion) {
        Date startTime = new Date();
        LOG.info("Start Processing for databases: " + Arrays.toString((config.getDatabases())));

        LOG.info(">>>>>>>>>>> Building/Starting Transition.");
        List<Future<ReturnStatus>> mdf = new ArrayList<Future<ReturnStatus>>();

        // Loop through databases
        Set<String> collectedDbs = conversion.getDatabases().keySet();
        for (String database : collectedDbs) {
            DBMirror dbMirror = conversion.getDatabase(database);
            // Loop through the tables in the database
            Set<String> tables = dbMirror.getTableMirrors().keySet();
            for (String table : tables) {
                TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
                switch (tblMirror.getPhaseState()) {
                    case INIT:
                    case STARTED:
                    case ERROR:
                        // Create a Transfer for the table.
                        Transfer md = new Transfer(config, dbMirror, tblMirror);
                        mdf.add(config.getTransferThreadPool().schedule(md, 1, TimeUnit.MILLISECONDS));
                        break;
                    case SUCCESS:
                        LOG.debug("DB.tbl: " + tblMirror.getDbName() + "." + tblMirror.getName(Environment.LEFT) + " was SUCCESSFUL in " +
                                "previous run.   SKIPPING and adjusting status to RETRY_SKIPPED_PAST_SUCCESS");
                        tblMirror.setPhaseState(PhaseState.RETRY_SKIPPED_PAST_SUCCESS);
                        break;
                    case RETRY_SKIPPED_PAST_SUCCESS:
                        LOG.debug("DB.tbl: " + tblMirror.getDbName() + "." + tblMirror.getName(Environment.LEFT) + " was SUCCESSFUL in " +
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

        Option quietOutput = new Option("q", "quiet", false,
                "Reduce screen reporting output.  Good for background processes with output redirects to a file");
        quietOutput.setOptionalArg(Boolean.FALSE);
        quietOutput.setRequired(Boolean.FALSE);
        options.addOption(quietOutput);

        Option metadataStage = new Option("d", "data-strategy", true,
                "Specify how the data will follow the schema. " + Arrays.deepToString(DataStrategy.visibleValues()));
        metadataStage.setOptionalArg(Boolean.TRUE);
        metadataStage.setArgName("strategy");
        metadataStage.setRequired(Boolean.FALSE);
        options.addOption(metadataStage);

        Option dumpSource = new Option("ds", "dump-source", true,
                "Specify which 'cluster' is the source for the DUMP strategy (LEFT|RIGHT). ");
        dumpSource.setOptionalArg(Boolean.TRUE);
        dumpSource.setArgName("source");
        dumpSource.setRequired(Boolean.FALSE);
        options.addOption(dumpSource);

        Option intermediateStorageOption = new Option("is", "intermediate-storage", true,
                "Intermediate Storage used with Data Strategy HYBRID, SQL, EXPORT_IMPORT.  This will change " +
                        "the way these methods are implemented by using the specified storage location as an " +
                        "intermediate transfer point between two clusters.  In this case, the cluster do NOT need to " +
                        "be 'linked'.  Each cluster DOES need to have access to the location and authorization to " +
                        "interact with the location.  This may mean additional configuration requirements for " +
                        "'hdfs' to ensure this seamless access.");
        intermediateStorageOption.setOptionalArg(Boolean.TRUE);
        intermediateStorageOption.setArgName("storage-path");
        intermediateStorageOption.setRequired(Boolean.FALSE);
        options.addOption(intermediateStorageOption);

        // Migration Options - Only one of these can be selected at a time, but isn't required.
        OptionGroup migrationOptionsGroup = new OptionGroup();
        migrationOptionsGroup.setRequired(Boolean.FALSE);

        Option dboOption = new Option("dbo", "database-only", false,
                "Migrate the Database definitions as they exist from LEFT to RIGHT");
        dboOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(dboOption);

        Option maoOption = new Option("mao", "migrate-acid-only", false,
                "Migrate ACID tables ONLY (if strategy allows). Optional: ArtificialBucketThreshold count that will remove " +
                        "the bucket definition if it's below this.  Use this as a way to remove artificial bucket definitions that " +
                        "were added 'artificially' in legacy Hive. (default: 2)");
        maoOption.setArgs(1);
        maoOption.setOptionalArg(Boolean.TRUE);
        maoOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(maoOption);

        Option mnnoOption = new Option("mnno", "migrate-non-native-only", false,
                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
                        "external connection to systems like: HBase, Kafka, JDBC");
        mnnoOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(mnnoOption);

        Option viewOption = new Option("v", "views-only", false,
                "Process VIEWs ONLY");
        viewOption.setRequired(false);
        migrationOptionsGroup.addOption(viewOption);

        options.addOptionGroup(migrationOptionsGroup);

        Option maOption = new Option("ma", "migrate-acid", false,
                "Migrate ACID tables (if strategy allows). Optional: ArtificialBucketThreshold count that will remove " +
                        "the bucket definition if it's below this.  Use this as a way to remove artificial bucket definitions that " +
                        "were added 'artificially' in legacy Hive. (default: 2)");
        maOption.setArgs(1);
        maOption.setOptionalArg(Boolean.TRUE);
        maOption.setRequired(Boolean.FALSE);
        options.addOption(maOption);

        Option daOption = new Option("da", "downgrade-acid", false,
                "Downgrade ACID tables to EXTERNAL tables with purge.");
        daOption.setRequired(Boolean.FALSE);
        options.addOption(daOption);

        // Non Native Migrations
        Option mnnOption = new Option("mnn", "migrate-non-native", false,
                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
                        "external connection to systems like: HBase, Kafka, JDBC");
        mnnOption.setArgs(1);
        mnnOption.setOptionalArg(Boolean.TRUE);
        mnnOption.setRequired(Boolean.FALSE);
        options.addOption(mnnOption);

        Option syncOption = new Option("s", "sync", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies.  Drop and Recreate Schema's when different.  " +
                        "Best to use with RO to ensure table/partition drops don't delete data. When used WITHOUT `-tf` it will " +
                        "compare all the tables in a database and sync (bi-directional).  Meaning it will DROP tables on the RIGHT " +
                        "that aren't in the LEFT and ADD tables to the RIGHT that are missing.  When used with `-ro`, table schemas can be updated " +
                        "by dropping and recreating.  When used with `-tf`, only the tables that match the filter (on both " +
                        "sides) will be considered.");
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

        Option outputOption = new Option("o", "output-dir", true,
                "Output Directory (default: $HOME/.hms-mirror/reports/<yyyy-MM-dd_HH-mm-ss>");
        outputOption.setRequired(Boolean.FALSE);
        outputOption.setArgName("outputdir");
        options.addOption(outputOption);

        Option skipFeaturesOption = new Option("sf", "skip-features", false,
                "Skip Features evaluation.");
        skipFeaturesOption.setRequired(Boolean.FALSE);
        options.addOption(skipFeaturesOption);

        Option executeOption = new Option("e", "execute", false,
                "Execute actions request, without this flag the process is a dry-run.");
        executeOption.setRequired(Boolean.FALSE);
        options.addOption(executeOption);

        Option asmOption = new Option("asm", "avro-schema-migration", false,
                "Migrate AVRO Schema Files referenced in TBLPROPERTIES by 'avro.schema.url'.  Without migration " +
                        "it is expected that the file will exist on the other cluster and match the 'url' defined in the " +
                        "schema DDL.\nIf it's not present, schema creation will FAIL.\nSpecifying this option REQUIRES the " +
                        "LEFT and RIGHT cluster to be LINKED.\nSee docs: https://github.com/dstreev/hms-mirror#linking-clusters-storage-layers");
        asmOption.setRequired(Boolean.FALSE);
        options.addOption(asmOption);

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

        Option pwOption = new Option("p", "password", true,
                "Used this in conjunction with '-pkey' to generate the encrypted password that you'll add to the configs for the JDBC connections.");
        pwOption.setRequired(Boolean.FALSE);
        pwOption.setArgName("password");
        options.addOption(pwOption);

        Option setupOption = new Option("su", "setup", false,
                "Setup a default configuration file through a series of questions");
        setupOption.setRequired(Boolean.FALSE);

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

        Option tableFilterOption = new Option("tf", "table-filter", true,
                "Filter tables (inclusive) with name matching RegEx. Comparison done with 'show tables' " +
                        "results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE.");
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


//        Option retryOption = new Option("r", "retry", false,
//                "Retry last incomplete run for 'cfg'.  If none specified, will check for 'default'");
//        retryOption.setRequired(false);
//        options.addOption(retryOption);

        return options;
    }

    public static void main(String[] args) {
        Mirror mirror = new Mirror();
        LOG.info("===================================================");
        LOG.info("Running: hms-mirror " + ReportingConf.substituteVariablesFromManifest("v.${Implementation-Version}"));
        LOG.info(" with commandline parameters: " + String.join(",", args));
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
