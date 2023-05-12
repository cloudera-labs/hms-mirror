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

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.stage.ReturnStatus;
import com.cloudera.utils.hadoop.hms.stage.Setup;
import com.cloudera.utils.hadoop.hms.stage.Transfer;
import com.cloudera.utils.hadoop.hms.util.Protect;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Sets;
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
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.ENVIRONMENT_CONNECTION_ISSUE;
import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.ENVIRONMENT_DISCONNECTED;

public class Mirror {
    private static final Logger LOG = LogManager.getLogger(Mirror.class);

    private Conversion conversion = null;
    private Config config = null;
    private String configFile = null;
    private String reportOutputDir = null;
    private String reportOutputFile = null;
    private String leftExecuteFile = null;
    private String leftCleanUpFile = null;
    private String rightExecuteFile = null;
    private String rightCleanUpFile = null;
    private final String leftActionFile = null;
    private final String rightActionFile = null;
    private final Boolean retry = Boolean.FALSE;
    private Boolean quiet = Boolean.FALSE;
    private String dateMarker;

    public Conversion getConversion() {
        return conversion;
    }

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

    public long init(String[] args) {
        long rtn = 0l;

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
                    "\nVisit https://github.com/cloudera-labs/hms-mirror/blob/main/README.md for detailed docs.");
//            formatter.printHelp(cmdline, options);
            throw new RuntimeException(pe);
        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = ReportingConf.substituteVariablesFromManifest("hms-mirror <options> \nversion:${Implementation-Version}");
            formatter.printHelp(100, cmdline, "Hive Metastore Migration Utility", options,
                    "\nVisit https://github.com/cloudera-labs/hms-mirror/blob/main/README.md for detailed docs");
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
            String yamlCfgFile = FileUtils.readFileToString(cfgFile, StandardCharsets.UTF_8);
            config = mapper.readerFor(Config.class).readValue(yamlCfgFile);
        } catch (Throwable t) {
            // Look for yaml update errors.
            if (t.toString().contains("MismatchedInputException")) {
                throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                        "'-su|--setup' again to recreate in the new format", t);
            } else {
//                config = new Config();
//                config.getErrors().set(CONFIGURATION_REMOVED_OR_INVALID.getCode(), t.getMessage());
                LOG.error(t);
                throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
            }
        }

        config.setCommandLineOptions(args);

        if (cmd.hasOption("p") || cmd.hasOption("dp")) {
            // Used to generate encrypted password.
            if (cmd.hasOption("pkey")) {
                Protect protect = new Protect(cmd.getOptionValue("pkey"));
                // Set to control execution flow.
                config.getErrors().set(MessageCode.PASSWORD_CFG.getCode());
                if (cmd.hasOption("p")) {
                    String epassword = null;
                    try {
                        epassword = protect.encrypt(cmd.getOptionValue("p"));
                        config.getWarnings().set(MessageCode.ENCRYPT_PASSWORD.getCode(), epassword);
                    } catch (Exception e) {
                        config.getErrors().set(MessageCode.ENCRYPT_PASSWORD_ISSUE.getCode());
//                        e.printStackTrace();
//                        System.exit(-1);
                    }
//                    System.out.println("Encrypted password: " + epassword);
                } else {
                    String password = null;
                    try {
                        password = protect.decrypt(cmd.getOptionValue("dp"));
                        config.getWarnings().set(MessageCode.DECRYPT_PASSWORD.getCode(), password);
                    } catch (Exception e) {
                        config.getErrors().set(MessageCode.DECRYPTING_PASSWORD_ISSUE.getCode());
                    }
//                    System.out.println("Original password (decrypted): " + password);
                }
            } else {
                config.getErrors().set(MessageCode.PKEY_PASSWORD_CFG.getCode());
            }
            return config.getErrors().getReturnCode();
        }

        if (cmd.hasOption("reset-right")) {
            config.setResetRight(Boolean.TRUE);
            config.setDatabaseOnly(Boolean.TRUE);
        } else {
            if (cmd.hasOption("f")) {
                config.setFlip(Boolean.TRUE);
            }

            if (cmd.hasOption("sf")) {
                // Skip Features.
                config.setSkipFeatures(Boolean.TRUE);
            }

            if (cmd.hasOption("q")) {
                // Skip Features.
                this.setQuiet(Boolean.TRUE);
            }

            if (cmd.hasOption("r")) {
                // replace
                config.setReplace(Boolean.TRUE);
            }

//        if (cmd.hasOption("t")) {
//            Translator translator = null;
//            File tCfgFile = new File(cmd.getOptionValue("t"));
//            if (!tCfgFile.exists()) {
//                throw new RuntimeException("Couldn't locate translation configuration file: " + cmd.getOptionValue("t"));
//            } else {
//                try {
//                    System.out.println("Using Translation Config: " + cmd.getOptionValue("t"));
//                    String yamlCfgFile = FileUtils.readFileToString(tCfgFile, Charset.forName("UTF-8"));
//                    translator = mapper.readerFor(Translator.class).readValue(yamlCfgFile);
//                    if (translator.validate()) {
//                        config.setTranslator(translator);
//                    } else {
//                        throw new RuntimeException("Translator config can't be validated, check logs.");
//                    }
//                } catch (Throwable t) {
//                    throw new RuntimeException(t);
//                }
//
//            }
//        }

            if (cmd.hasOption("rdl")) {
                config.setResetToDefaultLocation(Boolean.TRUE);
            }
            if (cmd.hasOption("fel")) {
                config.getTranslator().setForceExternalLocation(Boolean.TRUE);
            }
            if (cmd.hasOption("slt")) {
                config.setSkipLegacyTranslation(Boolean.TRUE);
            }

            if (cmd.hasOption("ap")) {
                config.getMigrateACID().setPartitionLimit(Integer.valueOf(cmd.getOptionValue("ap")));
            }

            if (cmd.hasOption("sp")) {
                config.getHybrid().setSqlPartitionLimit(Integer.valueOf(cmd.getOptionValue("sp")));
            }

            if (cmd.hasOption("ep")) {
                config.getHybrid().setExportImportPartitionLimit(Integer.valueOf(cmd.getOptionValue("ep")));
            }

            if (cmd.hasOption("dbp")) {
                config.setDbPrefix(cmd.getOptionValue("dbp"));
            }

            if (cmd.hasOption("dbr")) {
                config.setDbRename(cmd.getOptionValue("dbr"));
            }

            if (cmd.hasOption("v")) {
                config.getMigrateVIEW().setOn(Boolean.TRUE);
            }

            if (cmd.hasOption("dbo")) {
                config.setDatabaseOnly(Boolean.TRUE);
            }

            if (cmd.hasOption("slc")) {
                config.setSkipLinkCheck(Boolean.TRUE);
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
                if (cmd.hasOption("ip")) {
                    // Downgrade ACID tables
                    config.getMigrateACID().setInplace(Boolean.TRUE);
                    // For 'in-place' downgrade, only applies to ACID tables.
                    // Implies `-mao`.
                    config.getMigrateACID().setOn(Boolean.TRUE);
                }
            }

            if (cmd.hasOption("sdpi")) {
                config.getOptimization().setSortDynamicPartitionInserts(Boolean.TRUE);
            }

            if (cmd.hasOption("po")) {
                // property overrides.
                String[] overrides = cmd.getOptionValues("po");
                if (overrides != null)
                    config.getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.BOTH);
            }

            if (cmd.hasOption("pol")) {
                // property overrides.
                String[] overrides = cmd.getOptionValues("pol");
                if (overrides != null)
                    config.getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.LEFT);
            }

            if (cmd.hasOption("por")) {
                // property overrides.
                String[] overrides = cmd.getOptionValues("por");
                if (overrides != null)
                    config.getOptimization().getOverrides().setPropertyOverridesStr(overrides, Overrides.Side.RIGHT);
            }

            // Skip Optimizations.
            if (cmd.hasOption("so")) {
                config.getOptimization().setSkip(Boolean.TRUE);
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

            if (cmd.hasOption("to")) {
                config.setTransferOwnership(Boolean.TRUE);
            }

            if (cmd.hasOption("rid")) {
                config.getCluster(Environment.RIGHT).getHiveServer2().setDisconnected(Boolean.TRUE);
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
                if (cmd.hasOption("smn")) {
                    config.getTransfer().setCommonStorage(cmd.getOptionValue("smn"));
                }
                if (cmd.hasOption("sms")) {
                    try {
                        DataStrategy migrationStrategy = DataStrategy.valueOf(cmd.getOptionValue("sms"));
                        config.getTransfer().getStorageMigration().setStrategy(migrationStrategy);
                    } catch (Throwable t) {
                        LOG.error("Only SQL, EXPORT_IMPORT, and HYBRID are valid strategies for STORAGE_MIGRATION");
                        throw new RuntimeException("Only SQL, EXPORT_IMPORT, and HYBRID are valid strategies for STORAGE_MIGRATION");
                    }
                }
            }

            if (cmd.hasOption("wd")) {
                if (config.getTransfer().getWarehouse() == null)
                    config.getTransfer().setWarehouse(new WarehouseConfig());
                String wdStr = cmd.getOptionValue("wd");
                // Remove/prevent duplicate namespace config.
                if (config.getTransfer().getCommonStorage() != null) {
                    if (wdStr.startsWith(config.getTransfer().getCommonStorage())) {
                        wdStr = wdStr.substring(config.getTransfer().getCommonStorage().length());
                        LOG.warn("Managed Warehouse Location Modified (stripped duplicate namespace): " + wdStr);
                    }
                }
                config.getTransfer().getWarehouse().setManagedDirectory(wdStr);
            }

            if (cmd.hasOption("ewd")) {
                if (config.getTransfer().getWarehouse() == null)
                    config.getTransfer().setWarehouse(new WarehouseConfig());
                String ewdStr = cmd.getOptionValue("ewd");
                // Remove/prevent duplicate namespace config.
                if (config.getTransfer().getCommonStorage() != null) {
                    if (ewdStr.startsWith(config.getTransfer().getCommonStorage())) {
                        ewdStr = ewdStr.substring(config.getTransfer().getCommonStorage().length());
                        LOG.warn("External Warehouse Location Modified (stripped duplicate namespace): " + ewdStr);
                    }
                }
                config.getTransfer().getWarehouse().setExternalDirectory(ewdStr);
            }

            // GLOBAL (EXTERNAL) LOCATION MAP
            if (cmd.hasOption("glm")) {
                String[] globalLocMap = cmd.getOptionValues("glm");
                if (globalLocMap != null)
                    config.setGlobalLocationMapKV(globalLocMap);
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
                        // Don't process shadow, transfer clusters.
                        if (hiveServer2Config != null) {
                            Properties props = hiveServer2Config.getConnectionProperties();
                            String password = props.getProperty("password");
                            if (password != null) {
                                try {
                                    String decryptedPassword = protect.decrypt(password);
                                    props.put("password", decryptedPassword);
                                } catch (Exception e) {
                                    config.getErrors().set(MessageCode.PASSWORD_DECRYPT_ISSUE.getCode());
                                }
                            }
                        }
                    }
                }
                if (config.getErrors().getReturnCode() > 0)
                    return config.getErrors().getReturnCode();
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
                // This usually means an on-prem to cloud migration, which should be a PUSH data flow for distcp from the
                // LEFT and PULL from the RIGHT.
                config.getTransfer().getStorageMigration().setDataFlow(DistcpFlow.PUSH_PULL);
            }

            // Get intermediate Storage Location
            if (cmd.hasOption("cs")) {
                config.getTransfer().setCommonStorage(cmd.getOptionValue("cs"));
                // This usually means an on-prem to cloud migration, which should be a PUSH data flow for distcp.
                config.getTransfer().getStorageMigration().setDataFlow(DistcpFlow.PUSH);
            }

            // Set this after the is and cd checks.  Those will set default movement, but you can override here.
            if (cmd.hasOption("dc")) {
                config.getTransfer().getStorageMigration().setDistcp(Boolean.TRUE);
                String flowStr = cmd.getOptionValue("dc");
                if (flowStr != null) {
                    try {
                        DistcpFlow flow = DistcpFlow.valueOf(flowStr.toUpperCase(Locale.ROOT));
                        config.getTransfer().getStorageMigration().setDataFlow(flow);
                    } catch (IllegalArgumentException iae) {
                        throw new RuntimeException("Optional argument for `distcp` is invalid. Valid values: " +
                                Arrays.toString(DistcpFlow.values()), iae);
                    }
                }
            }

            if (cmd.hasOption("ro")) {
                switch (config.getDataStrategy()) {
                    case SCHEMA_ONLY:
                    case LINKED:
                    case COMMON:
                    case SQL:
                        config.setReadOnly(Boolean.TRUE);
                        break;
                    default:
                        throw new RuntimeException("RO option only valid with SCHEMA_ONLY, LINKED, SQL, and COMMON data strategies.");
                }
            }
            if (cmd.hasOption("np")) {
                config.setNoPurge(Boolean.TRUE);
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

            if (cmd.hasOption("tef")) {
                config.setTblExcludeRegEx(cmd.getOptionValue("tef"));
            }

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
        reportOutputFile = reportOutputDir + System.getProperty("file.separator") + "<db>_hms-mirror.md|html|yaml";
        leftExecuteFile = reportOutputDir + System.getProperty("file.separator") + "<db>_LEFT_execute.sql";
        leftCleanUpFile = reportOutputDir + System.getProperty("file.separator") + "<db>_LEFT_CleanUp_execute.sql";
        rightExecuteFile = reportOutputDir + System.getProperty("file.separator") + "<db>_RIGHT_execute.sql";
        rightCleanUpFile = reportOutputDir + System.getProperty("file.separator") + "<db>_RIGHT_CleanUp_execute.sql";

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
            throw new RuntimeException("Configuration issues., check log (~/.hms-mirror/logs/hms-mirror.log) for details");
        }

        ConnectionPools connPools = new ConnectionPools();
        Set<Environment> hs2Envs = new HashSet<Environment>();
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
            case STORAGE_MIGRATION:
                // Get Pool
                connPools.addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
                hs2Envs.add(Environment.LEFT);
                break;
            case SQL:
            case SCHEMA_ONLY:
            case EXPORT_IMPORT:
            case HYBRID:
                // When doing inplace downgrade of ACID tables, we're only dealing with the LEFT cluster.
                if (!config.getMigrateACID().isInplace()) {
                    connPools.addHiveServer2(Environment.RIGHT, config.getCluster(Environment.RIGHT).getHiveServer2());
                    hs2Envs.add(Environment.RIGHT);
                }
            default:
                connPools.addHiveServer2(Environment.LEFT, config.getCluster(Environment.LEFT).getHiveServer2());
                hs2Envs.add(Environment.LEFT);
                break;
        }
        try {
            connPools.init();
            for (Environment target : hs2Envs) {
                Connection conn = null;
                Statement stmt = null;
                try {
                    conn = connPools.getEnvironmentConnection(target);
                    if (conn == null) {
                        if (target == Environment.RIGHT && config.getCluster(target).getHiveServer2().isDisconnected()) {
                            // Skip error.  Set Warning that we're disconnected.
                            config.getWarnings().set(ENVIRONMENT_DISCONNECTED.getCode(), new Object[]{target});
                        } else {
                            config.getErrors().set(ENVIRONMENT_CONNECTION_ISSUE.getCode(), new Object[]{target});
                            return config.getErrors().getReturnCode();
                        }
                    } else {
                        // Exercise the connection.
                        stmt = conn.createStatement();
                        stmt.execute("SELECT 1");
                    }
                } catch (SQLException se) {
                    if (target == Environment.RIGHT && config.getCluster(target).getHiveServer2().isDisconnected()) {
                        // Set warning that RIGHT is disconnected.
                        config.getWarnings().set(ENVIRONMENT_DISCONNECTED.getCode(), new Object[]{target});
                    } else {
                        LOG.error(se);
                        config.getErrors().set(ENVIRONMENT_CONNECTION_ISSUE.getCode(), new Object[]{target});
                        return config.getErrors().getReturnCode();
                    }
                } catch (Throwable t) {
                    LOG.error(t);
                    config.getErrors().set(ENVIRONMENT_CONNECTION_ISSUE.getCode(), new Object[]{target});
                    return config.getErrors().getReturnCode();
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (conn != null) {
                        conn.close();
                    }
                }
            }
        } catch (SQLException cnfe) {
            LOG.error("Issue initializing connections.  Check driver locations", cnfe);
            throw new RuntimeException(cnfe);
        }

        config.getCluster(Environment.LEFT).setPools(connPools);
        switch (config.getDataStrategy()) {
            case DUMP:
                // Don't load the datasource for the right with DUMP strategy.
                break;
            default:
                // Don't set the Pools when Disconnected.
                if (config.getCluster(Environment.RIGHT).getHiveServer2() != null && !config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
                    config.getCluster(Environment.RIGHT).setPools(connPools);
                }
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
        return rtn;
    }

    public void doit() {
        conversion = new Conversion(config);

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
                retryCfgFile = FileUtils.readFileToString(retryFile, StandardCharsets.UTF_8);
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
        reporter.setVariable("left.cleanup.file", leftCleanUpFile);
        reporter.setVariable("right.execute.file", rightExecuteFile);
        reporter.setVariable("right.cleanup.file", rightCleanUpFile);
//        reporter.setVariable("left.action.file", leftActionFile);
//        reporter.setVariable("right.action.file", rightActionFile);
        reporter.setRetry(this.retry);
        reporter.start();

        Date startTime = new Date();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");

        if (config.isExecute()) {
            reporter.setVariable("run.mode", "EXECUTE");
        } else {
            reporter.setVariable("run.mode", "DRY-RUN");
        }
        Boolean setupError = Boolean.FALSE;

        // Skip Setup if working from 'retry'
        if (!retry) {
            // This will collect all existing DB/Table Definitions in the clusters
            Setup setup = new Setup(config, conversion);
            // TODO: Failure here may not make it to saved state.
            if (setup.collect()) {
//                stateMaintenance.saveState();
                // State reason table/view was removed from processing list.
                for (Map.Entry<String, DBMirror> dbEntry : conversion.getDatabases().entrySet()) {
                    if (config.getDatabaseOnly()) {
                        dbEntry.getValue().addIssue(Environment.RIGHT, "FYI:Only processing DB.");
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
            } else {
                setupError = Boolean.TRUE;
            }
        }

        // Remove the abstract environments from config before reporting output.
        config.getClusters().remove(Environment.TRANSFER);
        config.getClusters().remove(Environment.SHADOW);

        ObjectMapper mapper;
        mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        if (!setupError) {
            for (String database : config.getDatabases()) {

                String dbReportOutputFile = reportOutputDir + System.getProperty("file.separator") + database + "_hms-mirror";
                String dbLeftExecuteFile = reportOutputDir + System.getProperty("file.separator") + database + "_LEFT_execute.sql";
                String dbLeftCleanUpFile = reportOutputDir + System.getProperty("file.separator") + database + "_LEFT_CleanUp_execute.sql";
                String dbRightExecuteFile = reportOutputDir + System.getProperty("file.separator") + database + "_RIGHT_execute.sql";
                String dbRightCleanUpFile = reportOutputDir + System.getProperty("file.separator") + database + "_RIGHT_CleanUp_execute.sql";
                String dbRunbookFile = reportOutputDir + System.getProperty("file.separator") + database + "_runbook.md";

                try {
                    // Output directory maps
                    Boolean dcLeft = Boolean.FALSE;
                    Boolean dcRight = Boolean.FALSE;

                    if (config.canDeriveDistcpPlan()) {
                        try {
                            Environment[] environments = null;
                            switch (config.getDataStrategy()) {

                                case DUMP:
                                case STORAGE_MIGRATION:
                                    environments = new Environment[]{Environment.LEFT};
                                    break;
                                default:
                                    environments = new Environment[]{Environment.LEFT, Environment.RIGHT};
                                    break;
                            }

                            for (Environment distcpEnv : environments) {
                                Boolean dcFound = Boolean.FALSE;

                                StringBuilder distcpWorkbookSb = new StringBuilder();
                                StringBuilder distcpScriptSb = new StringBuilder();

                                distcpScriptSb.append("#!/usr/bin/env sh").append("\n");
                                distcpScriptSb.append("\n");
                                distcpScriptSb.append("# 1. Copy the source '*_distcp_source.txt' files to the distributed filesystem.").append("\n");
                                distcpScriptSb.append("# 2. Export an env var 'HCFS_BASE_DIR' that represents where these files where placed.").append("\n");
                                distcpScriptSb.append("#      NOTE: ${HCFS_BASE_DIR} must be available to the user running 'distcp'").append("\n");
                                distcpScriptSb.append("# 3. Export an env var 'DISTCP_OPTS' with any special settings needed to run the job.").append("\n");
                                distcpScriptSb.append("#      For large jobs, you may need to adjust memory settings.").append("\n");
                                distcpScriptSb.append("# 4. Run the following in an order or framework that is appropriate for your environment.").append("\n");
                                distcpScriptSb.append("#       These aren't necessarily expected to run in this shell script as is in production.").append("\n");
                                distcpScriptSb.append("\n");
                                distcpScriptSb.append("\n");
                                distcpScriptSb.append("if [ -z ${HCFS_BASE_DIR+x} ]; then").append("\n");
                                distcpScriptSb.append("  echo \"HCFS_BASE_DIR is unset\"").append("\n");
                                distcpScriptSb.append("  echo \"What is the 'HCFS_BASE_DIR':\"").append("\n");
                                distcpScriptSb.append("  read HCFS_BASE_DIR").append("\n");
                                distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
                                distcpScriptSb.append("else").append("\n");
                                distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
                                distcpScriptSb.append("fi").append("\n");
                                distcpScriptSb.append("\n");
                                distcpScriptSb.append("echo \"Creating HCFS directory: $HCFS_BASE_DIR\"").append("\n");
                                distcpScriptSb.append("hdfs dfs -mkdir -p $HCFS_BASE_DIR").append("\n");
                                distcpScriptSb.append("\n");

                                // WARNING ABOUT 'distcp' and 'table alignment'
                                distcpWorkbookSb.append("## WARNING\n");
                                distcpWorkbookSb.append(MessageCode.RDL_DC_WARNING_TABLE_ALIGNMENT.getDesc()).append("\n\n");

                                distcpWorkbookSb.append("| Database | Target | Sources |\n");
                                distcpWorkbookSb.append("|:---|:---|:---|\n");

                                FileWriter distcpSourceFW = null;
                                for (Map.Entry<String, Map<String, Set<String>>> entry :
                                        config.getTranslator().buildDistcpList(database, distcpEnv, 1).entrySet()) {

                                    distcpWorkbookSb.append("| " + entry.getKey() + " | | |\n");
                                    Map<String, Set<String>> value = entry.getValue();
                                    int i = 1;
                                    for (Map.Entry<String, Set<String>> dbMap : value.entrySet()) {
                                        String distcpSourceFile = entry.getKey() + "_" + distcpEnv.toString() + "_" + i++ + "_distcp_source.txt";
                                        String distcpSourceFileFull = reportOutputDir + System.getProperty("file.separator") + distcpSourceFile;
                                        distcpSourceFW = new FileWriter(distcpSourceFileFull);

                                        StringBuilder line = new StringBuilder();
                                        line.append("| | ").append(dbMap.getKey()).append(" | ");

                                        for (String source : dbMap.getValue()) {
                                            line.append(source).append("<br>");
                                            distcpSourceFW.append(source).append("\n");
                                        }
                                        line.append(" | ").append("\n");
                                        distcpWorkbookSb.append(line);

                                        distcpScriptSb.append("\n");
                                        distcpScriptSb.append("echo \"Copying 'distcp' source file to $HCFS_BASE_DIR\"").append("\n");
                                        distcpScriptSb.append("\n");
                                        distcpScriptSb.append("hdfs dfs -copyFromLocal -f " + distcpSourceFile + " ${HCFS_BASE_DIR}").append("\n");
                                        distcpScriptSb.append("\n");
                                        distcpScriptSb.append("echo \"Running 'distcp'\"").append("\n");
                                        distcpScriptSb.append("hadoop distcp ${DISTCP_OPTS} -f ${HCFS_BASE_DIR}/" + distcpSourceFile + " " +
                                                dbMap.getKey() + "\n").append("\n");

                                        distcpSourceFW.close();

                                        dcFound = Boolean.TRUE;
                                    }
                                }

                                if (dcFound) {
                                    // Set flags for report and workplan
                                    switch (distcpEnv) {
                                        case LEFT:
                                            dcLeft = Boolean.TRUE;
                                            break;
                                        case RIGHT:
                                            dcRight = Boolean.TRUE;
                                            break;
                                    }

                                    String distcpWorkbookFile = reportOutputDir + System.getProperty("file.separator") + database +
                                            "_" + distcpEnv + "_distcp_workbook.md";
                                    String distcpScriptFile = reportOutputDir + System.getProperty("file.separator") + database +
                                            "_" + distcpEnv + "_distcp_script.sh";

                                    FileWriter distcpWorkbookFW = new FileWriter(distcpWorkbookFile);
                                    FileWriter distcpScriptFW = new FileWriter(distcpScriptFile);

                                    distcpScriptFW.write(distcpScriptSb.toString());
                                    distcpWorkbookFW.write(distcpWorkbookSb.toString());

                                    distcpScriptFW.close();
                                    distcpWorkbookFW.close();
                                }
                            }
                        } catch (IOException ioe) {
                            LOG.error("Issue writing distcp workbook", ioe);
                        }
                    }


                    FileWriter runbookFile = new FileWriter(dbRunbookFile);
                    runbookFile.write("# Runbook for database: " + database);
                    runbookFile.write("\n\nYou'll find the **run report** in the file:\n\n`" + dbReportOutputFile + ".md|html` " +
                            "\n\nThis file includes details about the configuration at the time this was run and the " +
                            "output/actions on each table in the database that was included.\n\n");
                    runbookFile.write("## Steps\n\n");
                    if (config.isExecute()) {
                        runbookFile.write("Execute was **ON**, so many of the scripts have been run already.  Verify status " +
                                "in the above report.  `distcp` actions (if requested/applicable) need to be run manually. " +
                                "Some cleanup scripts may have been run if no `distcp` actions were requested.\n\n");
                        if (config.getCluster(Environment.RIGHT).getHiveServer2() != null) {
                            if (config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
                                runbookFile.write("Process ran with RIGHT environment 'disconnected'.  All RIGHT scripts will need to be run manually.\n\n");
                            }
                        }
                    } else {
                        runbookFile.write("Execute was **OFF**.  All actions will need to be run manually. See below steps.\n\n");
                    }
                    int step = 1;
                    FileWriter reportFile = new FileWriter(dbReportOutputFile + ".md");
                    String mdReportStr = conversion.toReport(config, database);


                    File dbYamlFile = new File(dbReportOutputFile + ".yaml");
                    FileWriter dbYamlFileWriter = new FileWriter(dbYamlFile);

                    DBMirror yamlDb = conversion.getDatabase(database);

                    String dbYamlStr = mapper.writeValueAsString(yamlDb);
                    try {
                        dbYamlFileWriter.write(dbYamlStr);
                        LOG.info("Database (" + database + ") yaml 'saved' to: " + dbYamlFile.getPath());
                    } catch (IOException ioe) {
                        LOG.error("Problem 'writing' database yaml", ioe);
                    } finally {
                        dbYamlFileWriter.close();
                    }

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

                    LOG.info("Status Report of 'hms-mirror' is here: " + dbReportOutputFile + ".md|html");

                    String les = conversion.executeSql(Environment.LEFT, database);
                    if (les != null) {
                        FileWriter leftExecOutput = new FileWriter(dbLeftExecuteFile);
                        leftExecOutput.write(les);
                        leftExecOutput.close();
                        LOG.info("LEFT Execution Script is here: " + dbLeftExecuteFile);
                        runbookFile.write(step++ + ". **LEFT** clusters SQL script. ");
                        if (config.isExecute()) {
                            runbookFile.write(" (Has been executed already, check report file details)");
                        } else {
                            runbookFile.write("(Has NOT been executed yet)");
                        }
                        runbookFile.write("\n");
                    }

                    if (dcLeft) {
                        runbookFile.write(step++ + ". **LEFT** cluster `distcp` actions.  Needs to be performed manually.  Use 'distcp' report/template.");
                        runbookFile.write("\n");
                    }

                    String res = conversion.executeSql(Environment.RIGHT, database);
                    if (res != null) {
                        FileWriter rightExecOutput = new FileWriter(dbRightExecuteFile);
                        rightExecOutput.write(res);
                        rightExecOutput.close();
                        LOG.info("RIGHT Execution Script is here: " + dbRightExecuteFile);
                        runbookFile.write(step++ + ". **RIGHT** clusters SQL script. ");
                        if (config.isExecute()) {
                            if (!config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
                                runbookFile.write(" (Has been executed already, check report file details)");
                            } else {
                                runbookFile.write(" (Has NOT been executed because the environment is NOT connected.  Review and run scripts manually.)");
                            }
                        } else {
                            runbookFile.write("(Has NOT been executed yet)");
                        }
                        runbookFile.write("\n");
                    }

                    if (dcRight) {
                        runbookFile.write(step++ + ". **RIGHT** cluster `distcp` actions.  Needs to be performed manually.  Use 'distcp' report/template.");
                        runbookFile.write("\n");
                    }

                    String lcu = conversion.executeCleanUpSql(Environment.LEFT, database);
                    if (lcu != null) {
                        FileWriter leftCleanUpOutput = new FileWriter(dbLeftCleanUpFile);
                        leftCleanUpOutput.write(lcu);
                        leftCleanUpOutput.close();
                        LOG.info("LEFT CleanUp Execution Script is here: " + dbLeftCleanUpFile);
                        runbookFile.write(step++ + ". **LEFT** clusters CLEANUP SQL script. ");
                        runbookFile.write("(Has NOT been executed yet)");
                        runbookFile.write("\n");
                    }

                    String rcu = conversion.executeCleanUpSql(Environment.RIGHT, database);
                    if (rcu != null) {
                        FileWriter rightCleanUpOutput = new FileWriter(dbRightCleanUpFile);
                        rightCleanUpOutput.write(rcu);
                        rightCleanUpOutput.close();
                        LOG.info("RIGHT CleanUp Execution Script is here: " + dbRightCleanUpFile);
                        runbookFile.write(step++ + ". **RIGHT** clusters CLEANUP SQL script. ");
                        runbookFile.write("(Has NOT been executed yet)");
                        runbookFile.write("\n");
                    }
                    LOG.info("Runbook here: " + dbRunbookFile);
                    runbookFile.close();
                } catch (IOException ioe) {
                    LOG.error("Issue writing report for: " + database, ioe);
                }
            }
        }
        Date endTime = new Date();
        DecimalFormat decf = new DecimalFormat("#.###");
        decf.setRoundingMode(RoundingMode.CEILING);

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
                        switch (sf.get().getStatus()) {
                            case SUCCESS:
                                break;
                            case ERROR:
                            case FATAL:
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

        Option resetTarget = new Option("rr", "reset-right", false,
                "Use this for testing to remove the database on the RIGHT using CASCADE.");
        resetTarget.setRequired(Boolean.FALSE);
        options.addOption(resetTarget);

        Option resetToDefaultLocation = new Option("rdl", "reset-to-default-location", false,
                "Strip 'LOCATION' from all target cluster definitions.  This will allow the system defaults " +
                        "to take over and define the location of the new datasets.");
        resetToDefaultLocation.setRequired(Boolean.FALSE);
        options.addOption(resetToDefaultLocation);

        Option skipLegacyTranslation = new Option("slt", "skip-legacy-translation", false,
                "Skip Schema Upgrades and Serde Translations");
        skipLegacyTranslation.setRequired(Boolean.FALSE);
        options.addOption(skipLegacyTranslation);

        Option flipOption = new Option("f", "flip", false,
                "Flip the definitions for LEFT and RIGHT.  Allows the same config to be used in reverse.");
        flipOption.setOptionalArg(Boolean.FALSE);
        flipOption.setRequired(Boolean.FALSE);
        options.addOption(flipOption);

        Option smDistCpOption = new Option("dc", "distcp", false,
                "Build the 'distcp' workplans.  Optional argument (PULL, PUSH) to define which cluster is running " +
                        "the distcp commands.  Default is PULL.");
        smDistCpOption.setArgs(1);
        smDistCpOption.setOptionalArg(Boolean.TRUE);
        smDistCpOption.setArgName("flow-direction default:PULL");
        smDistCpOption.setRequired(Boolean.FALSE);
        options.addOption(smDistCpOption);

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

        Option propertyOverrides = new Option("po", "property-overrides", true,
                "Comma separated key=value pairs of Hive properties you wish to set/override.");
        propertyOverrides.setArgName("key=value");
        propertyOverrides.setRequired(Boolean.FALSE);
        propertyOverrides.setValueSeparator(',');
        propertyOverrides.setArgs(100);
        options.addOption(propertyOverrides);

        Option propertyLeftOverrides = new Option("pol", "property-overrides-left", true,
                "Comma separated key=value pairs of Hive properties you wish to set/override for LEFT cluster.");
        propertyLeftOverrides.setArgName("key=value");
        propertyLeftOverrides.setRequired(Boolean.FALSE);
        propertyLeftOverrides.setValueSeparator(',');
        propertyLeftOverrides.setArgs(100);
        options.addOption(propertyLeftOverrides);

        Option propertyRightOverrides = new Option("por", "property-overrides-right", true,
                "Comma separated key=value pairs of Hive properties you wish to set/override for RIGHT cluster.");
        propertyRightOverrides.setArgName("key=value");
        propertyRightOverrides.setRequired(Boolean.FALSE);
        propertyRightOverrides.setValueSeparator(',');
        propertyRightOverrides.setArgs(100);
        options.addOption(propertyRightOverrides);

        Option skipOptimizationsOption = new Option("so", "skip-optimizations", false,
                "Skip any optimizations during data movement, like dynamic sorting or distribute by");
        skipOptimizationsOption.setRequired(Boolean.FALSE);
        options.addOption(skipOptimizationsOption);

        Option forceExternalLocationOption = new Option("fel", "force-external-location", false,
                "Under some conditions, the LOCATION element for EXTERNAL tables is removed (ie: -rdl).  " +
                        "In which case we rely on the settings of the database definition to control the " +
                        "EXTERNAL table data location.  But for some older Hive versions, the LOCATION element in " +
                        "the database is NOT honored.  Even when the database LOCATION is set, the EXTERNAL table LOCATION " +
                        "defaults to the system wide warehouse settings.  This flag will ensure the LOCATION element " +
                        "remains in the CREATE definition of the table to force it's location.");
        forceExternalLocationOption.setRequired(Boolean.FALSE);
        options.addOption(forceExternalLocationOption);

        Option glblLocationMapOption = new Option("glm", "global-location-map", true,
                "Comma separated key=value pairs of Locations to Map. IE: /myorig/data/finance=/data/ec/finance. " +
                        "This reviews 'EXTERNAL' table locations for the path '/myorig/data/finance' and replaces it " +
                        "with '/data/ec/finance'.  Option can be used alone or with -rdl. Only applies to 'EXTERNAL' tables " +
                        "and if the tables location doesn't contain one of the supplied maps, it will be translated according " +
                        "to -rdl rules if -rdl is specified.  If -rdl is not specified, the conversion for that table is skipped. ");
        glblLocationMapOption.setArgName("key=value");
        glblLocationMapOption.setRequired(Boolean.FALSE);
        glblLocationMapOption.setValueSeparator(',');
        glblLocationMapOption.setArgs(1000);
        options.addOption(glblLocationMapOption);

        OptionGroup storageOptionsGroup = new OptionGroup();
        storageOptionsGroup.setRequired(Boolean.FALSE);

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
        storageOptionsGroup.addOption(intermediateStorageOption);

        Option commonStorageOption = new Option("cs", "common-storage", true,
                "Common Storage used with Data Strategy HYBRID, SQL, EXPORT_IMPORT.  This will change " +
                        "the way these methods are implemented by using the specified storage location as an " +
                        "'common' storage point between two clusters.  In this case, the cluster do NOT need to " +
                        "be 'linked'.  Each cluster DOES need to have access to the location and authorization to " +
                        "interact with the location.  This may mean additional configuration requirements for " +
                        "'hdfs' to ensure this seamless access.");
        commonStorageOption.setOptionalArg(Boolean.TRUE);
        commonStorageOption.setArgName("storage-path");
        commonStorageOption.setRequired(Boolean.FALSE);
        storageOptionsGroup.addOption(commonStorageOption);

        options.addOptionGroup(storageOptionsGroup);

        // External Warehouse Dir
        Option externalWarehouseDirOption = new Option("ewd", "external-warehouse-directory", true,
                "The external warehouse directory path.  Should not include the namespace OR the database directory. " +
                        "This will be used to set the LOCATION database option.");
        externalWarehouseDirOption.setOptionalArg(Boolean.TRUE);
        externalWarehouseDirOption.setArgName("path");
        externalWarehouseDirOption.setRequired(Boolean.FALSE);
        options.addOption(externalWarehouseDirOption);

        // Warehouse Dir
        Option warehouseDirOption = new Option("wd", "warehouse-directory", true,
                "The warehouse directory path.  Should not include the namespace OR the database directory. " +
                        "This will be used to set the MANAGEDLOCATION database option.");
        warehouseDirOption.setOptionalArg(Boolean.TRUE);
        warehouseDirOption.setArgName("path");
        warehouseDirOption.setRequired(Boolean.FALSE);
        options.addOption(warehouseDirOption);

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
        maoOption.setArgName("bucket-threshold (2)");
        maoOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(maoOption);

        Option mnnoOption = new Option("mnno", "migrate-non-native-only", false,
                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
                        "external connection to systems like: HBase, Kafka, JDBC");
        mnnoOption.setRequired(Boolean.FALSE);
        migrationOptionsGroup.addOption(mnnoOption);

        Option sdpiOption = new Option("sdpi", "sort-dynamic-partition-inserts", false,
                "Used to set `hive.optimize.sort.dynamic.partition` in TEZ for optimal partition inserts.  " +
                        "When not specified, will use prescriptive sorting by adding 'DISTRIBUTE BY' to transfer SQL. " +
                        "default: false");
        sdpiOption.setRequired(Boolean.FALSE);
        options.addOption(sdpiOption);

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
        maOption.setArgName("bucket-threshold (2)");
        maOption.setRequired(Boolean.FALSE);
        options.addOption(maOption);

        Option daOption = new Option("da", "downgrade-acid", false,
                "Downgrade ACID tables to EXTERNAL tables with purge.");
        daOption.setRequired(Boolean.FALSE);
        options.addOption(daOption);

        Option ridOption = new Option("rid", "right-is-disconnected", false,
                "Don't attempt to connect to the 'right' cluster and run in this mode");
        ridOption.setRequired(Boolean.FALSE);
        options.addOption(ridOption);

        Option ipOption = new Option("ip", "in-place", false,
                "Downgrade ACID tables to EXTERNAL tables with purge.");
        ipOption.setRequired(Boolean.FALSE);
        options.addOption(ipOption);

        Option skipLinkTestOption = new Option("slc", "skip-link-check", false,
                "Skip Link Check. Use when going between or to Cloud Storage to avoid having to configure " +
                        "hms-mirror with storage credentials and libraries. This does NOT preclude your Hive Server 2 and " +
                        "compute environment from such requirements.");
        skipLinkTestOption.setRequired(Boolean.FALSE);
        options.addOption(skipLinkTestOption);

        // Non Native Migrations
        Option mnnOption = new Option("mnn", "migrate-non-native", false,
                "Migrate Non-Native tables (if strategy allows). These include table definitions that rely on " +
                        "external connection to systems like: HBase, Kafka, JDBC");
        mnnOption.setArgs(1);
        mnnOption.setOptionalArg(Boolean.TRUE);
        mnnOption.setRequired(Boolean.FALSE);
        options.addOption(mnnOption);

        // TODO: Implement this feature...  If requested.  Needs testings, not complete after other downgrade work.
//        Option replaceOption = new Option("r", "replace", false,
//                "When downgrading an ACID table as its transferred to the 'RIGHT' cluster, this option " +
//                        "will replace the current ACID table on the LEFT cluster with a 'downgraded' table (EXTERNAL). " +
//                        "The option only works with options '-da' and '-cs'.");
//        replaceOption.setRequired(Boolean.FALSE);
//        options.addOption(replaceOption);

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
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP. " +
                        "Intended for use with replication distcp strategies and has restrictions about existing DB's " +
                        "on RIGHT and PATH elements.  To simply NOT set the purge flag for applicable tables, use -np.");
        roOption.setRequired(Boolean.FALSE);
        options.addOption(roOption);

        Option npOption = new Option("np", "no-purge", false,
                "For SCHEMA_ONLY, COMMON, and LINKED data strategies set RIGHT table to NOT purge on DROP");
        npOption.setRequired(Boolean.FALSE);
        options.addOption(npOption);

        Option acceptOption = new Option("accept", "accept", false,
                "Accept ALL confirmations and silence prompts");
        acceptOption.setRequired(Boolean.FALSE);
        options.addOption(acceptOption);

        // TODO: Add addition Storage Migration Strategies (current default and only option is SQL)
//        Option translateConfigOption = new Option("t", "translate-config", true,
//                "Translator Configuration File (Experimental)");
//        translateConfigOption.setRequired(Boolean.FALSE);
//        translateConfigOption.setArgName("translate-config-file");
//        options.addOption(translateConfigOption);

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
                        "LEFT and RIGHT cluster to be LINKED.\nSee docs: https://github.com/cloudera-labs/hms-mirror#linking-clusters-storage-layers");
        asmOption.setRequired(Boolean.FALSE);
        options.addOption(asmOption);

        Option transferOwnershipOption = new Option("to", "transfer-ownership", false,
                "If available (supported) on LEFT cluster, extract and transfer the tables owner to the " +
                        "RIGHT cluster. Note: This will make an 'exta' SQL call on the LEFT cluster to determine " +
                        "the ownership.  This won't be supported on CDH 5 and some other legacy Hive platforms. " +
                        "Beware the cost of this extra call for EVERY table, as it may slow down the process for " +
                        "a large volume of tables.");
        transferOwnershipOption.setRequired(Boolean.FALSE);
        options.addOption(transferOwnershipOption);

        OptionGroup dbAdjustOptionGroup = new OptionGroup();

        Option dbPrefixOption = new Option("dbp", "db-prefix", true,
                "Optional: A prefix to add to the RIGHT cluster DB Name. Usually used for testing.");
        dbPrefixOption.setRequired(Boolean.FALSE);
        dbPrefixOption.setArgName("prefix");
        dbAdjustOptionGroup.addOption(dbPrefixOption);

        Option dbRenameOption = new Option("dbr", "db-rename", true,
                "Optional: Rename target db to ...  This option is only valid when '1' database is listed in `-db`.");
        dbRenameOption.setRequired(Boolean.FALSE);
        dbRenameOption.setArgName("rename");
        dbAdjustOptionGroup.addOption(dbRenameOption);

        options.addOptionGroup(dbAdjustOptionGroup);

        Option storageMigrationNamespaceOption = new Option("smn", "storage-migration-namespace", true,
                "Optional: Used with the 'data strategy STORAGE_MIGRATION to specify the target namespace.");
        storageMigrationNamespaceOption.setRequired(Boolean.FALSE);
        storageMigrationNamespaceOption.setArgName("namespace");
        options.addOption(storageMigrationNamespaceOption);

//        Option storageMigrationStrategyOption = new Option("sms", "storage-migration-strategy", true,
//                "Optional: Used with the 'data strategy' STORAGE_MIGRATION to specify the technique used to migration.  " +
//                        "Options are: [SQL,EXPORT_IMPORT,HYBRID]. Default is SQL");
//        storageMigrationStrategyOption.setRequired(Boolean.FALSE);
//        storageMigrationStrategyOption.setArgName("Storage Migration Strategy");
//        options.addOption(storageMigrationStrategyOption);

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

        Option decryptPWOption = new Option("dp", "decrypt-password", true,
                "Used this in conjunction with '-pkey' to decrypt the generated passcode from `-p`.");
        decryptPWOption.setRequired(Boolean.FALSE);
        decryptPWOption.setArgName("encrypted-password");

        Option setupOption = new Option("su", "setup", false,
                "Setup a default configuration file through a series of questions");
        setupOption.setRequired(Boolean.FALSE);

        Option pKeyOption = new Option("pkey", "password-key", true,
                "The key used to encrypt / decrypt the cluster jdbc passwords.  If not present, the passwords will be processed as is (clear text) from the config file.");
        pKeyOption.setRequired(false);
        pKeyOption.setArgName("password-key");
        options.addOption(pKeyOption);

        OptionGroup dbGroup = new OptionGroup();
        dbGroup.addOption(dbOption);
        dbGroup.addOption(helpOption);
        dbGroup.addOption(setupOption);
        dbGroup.addOption(pwOption);
        dbGroup.addOption(decryptPWOption);
        dbGroup.setRequired(Boolean.TRUE);
        options.addOptionGroup(dbGroup);

        Option sqlOutputOption = new Option("sql", "sql-output", false,
                "<deprecated>.  This option is no longer required to get SQL out in a report.  That is the default behavior.");
        sqlOutputOption.setRequired(Boolean.FALSE);
        options.addOption(sqlOutputOption);

        Option acidPartCountOption = new Option("ap", "acid-partition-count", true,
                "Set the limit of partitions that the ACID strategy will work with. '-1' means no-limit.");
        acidPartCountOption.setRequired(Boolean.FALSE);
        acidPartCountOption.setArgName("limit");
        options.addOption(acidPartCountOption);

        Option sqlPartCountOption = new Option("sp", "sql-partition-count", true,
                "Set the limit of partitions that the SQL strategy will work with. '-1' means no-limit.");
        sqlPartCountOption.setRequired(Boolean.FALSE);
        sqlPartCountOption.setArgName("limit");
        options.addOption(sqlPartCountOption);

        Option expImpPartCountOption = new Option("ep", "export-partition-count", true,
                "Set the limit of partitions that the EXPORT_IMPORT strategy will work with.");
        expImpPartCountOption.setRequired(Boolean.FALSE);
        expImpPartCountOption.setArgName("limit");
        options.addOption(expImpPartCountOption);

        OptionGroup filterGroup = new OptionGroup();
        filterGroup.setRequired(Boolean.FALSE);

        Option tableFilterOption = new Option("tf", "table-filter", true,
                "Filter tables (inclusive) with name matching RegEx. Comparison done with 'show tables' " +
                        "results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE. " +
                        "Make sure you double-quote the expression on the commandline.");
        tableFilterOption.setRequired(Boolean.FALSE);
        tableFilterOption.setArgName("regex");
        filterGroup.addOption(tableFilterOption);

        Option excludeTableFilterOption = new Option("tef", "table-exclude-filter", true,
                "Filter tables (excludes) with name matching RegEx. Comparison done with 'show tables' " +
                        "results.  Check case, that's important.  Hive tables are generally stored in LOWERCASE. " +
                        "Make sure you double-quote the expression on the commandline.");
        excludeTableFilterOption.setRequired(Boolean.FALSE);
        excludeTableFilterOption.setArgName("regex");
        filterGroup.addOption(excludeTableFilterOption);

        options.addOptionGroup(filterGroup);

        Option cfgOption = new Option("cfg", "config", true,
                "Config with details for the HMS-Mirror.  Default: $HOME/.hms-mirror/cfg/default.yaml");
        cfgOption.setRequired(false);
        cfgOption.setArgName("filename");
        options.addOption(cfgOption);

        return options;
    }

    protected Boolean setupSql(Environment environment, List<Pair> sqlPairList) {
        Boolean rtn = Boolean.TRUE;
        rtn = config.getCluster(environment).runClusterSql(sqlPairList);
        return rtn;
    }

    protected long setupSqlLeft(String[] args, List<Pair> sqlPairList) {
        long rtn = 0l;
        rtn = setupSql(args, sqlPairList, null);
        return rtn;
    }

    protected long setupSqlRight(String[] args, List<Pair> sqlPairList) {
        long rtn = 0l;
        rtn = setupSql(args, null, sqlPairList);
        return rtn;
    }

    public long setupSql(String[] args, List<Pair> leftSql, List<Pair> rightSql) {
        long returnCode = 0;
        LOG.info("===================================================");
        LOG.info("Running: hms-mirror " + ReportingConf.substituteVariablesFromManifest("v.${Implementation-Version}"));
        LOG.info(" with commandline parameters: " + String.join(",", args));
        LOG.info("===================================================");
        LOG.info("");
        LOG.info("======  SQL Setup ======");
        try {
            returnCode = init(args);
            try {
                if (leftSql != null && leftSql.size() > 0) {
                    if (!setupSql(Environment.LEFT, leftSql)) {
                        LOG.error("Failed to run LEFT SQL, check Logs");
                        returnCode = -1;
                    }
                }
                if (rightSql != null && rightSql.size() > 0) {
                    if (!setupSql(Environment.RIGHT, rightSql)) {
                        LOG.error("Failed to run RIGHT SQL, check Logs");
                        returnCode = -1;
                    }
                }
            } catch (RuntimeException rte) {
                System.out.println(rte.getMessage());
                rte.printStackTrace();
                if (config != null) {
                    returnCode = config.getErrors().getReturnCode(); //MessageCode.returnCode(config.getErrors());
                } else {
                    returnCode = -1;
                }
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            System.err.println("=====================================================");
            System.err.println("Commandline args: " + Arrays.toString(args));
            System.err.println();
            LOG.error("Commandline args: " + Arrays.toString(args));
            if (config != null) {
                for (String error : config.getErrors().getMessages()) {
                    LOG.error(error);
                    System.err.println(error);
                }
                returnCode = config.getErrors().getReturnCode();
            } else {
                returnCode = -1;
            }
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.err.println("\nSee log for stack trace ($HOME/.hms-mirror/logs)");
        }
        return returnCode;
    }

    public long go(String[] args) {
        long returnCode = 0;
        LOG.info("===================================================");
        LOG.info("Running: hms-mirror " + ReportingConf.substituteVariablesFromManifest("v.${Implementation-Version}"));
        LOG.info(" with commandline parameters: " + String.join(",", args));
        LOG.info("===================================================");
        try {
            returnCode = init(args);
            try {
                if (returnCode == 0)
                    doit();
            } catch (RuntimeException rte) {
                System.out.println(rte.getMessage());
                rte.printStackTrace();
                if (config != null && config.getErrors().getReturnCode() > 0) {
                    returnCode = config.getErrors().getReturnCode(); //MessageCode.returnCode(config.getErrors());
                } else {
                    returnCode = -1;
                }
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            System.err.println("=====================================================");
            System.err.println("Commandline args: " + Arrays.toString(args));
            System.err.println();
            LOG.error("Commandline args: " + Arrays.toString(args));
            if (config != null) {
                returnCode = config.getErrors().getReturnCode();
            } else {
                returnCode = -1;
            }
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.err.println("\nSee log for stack trace ($HOME/.hms-mirror/logs)");
        } finally {
            if (config != null) {
                if (config.getErrors().getReturnCode() != 0) {
                    System.err.println("******* ERRORS *********");
                }
                for (String error : config.getErrors().getMessages()) {
                    LOG.error(error);
                    System.err.println(error);
                }
                if (config.getWarnings().getReturnCode() != 0) {
                    System.err.println("******* WARNINGS *********");
                }
                for (String warning : config.getWarnings().getMessages()) {
                    LOG.warn(warning);
                    System.err.println(warning);
                }
            }
        }
        return returnCode;
    }

    public static void main(String[] args) {
        Mirror mirror = new Mirror();
        System.exit((int) mirror.go(args));
    }
}
