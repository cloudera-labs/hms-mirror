package com.streever.hadoop.hms;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.streever.hadoop.hms.mirror.*;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Arrays;

public class Mirror {
    private static Logger LOG = LogManager.getLogger(Mirror.class);
    private String[] databases = null;
    private String configFile = null;
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
            System.out.println("Running Metadata");
            LOG.info("Running Metadata");
        } else if (cmd.hasOption("s")) {
            stage = Stage.STORAGE;
            System.out.println("Running Storage");
            LOG.info("Running Storage");
        } else {
            throw new RuntimeException("Need to specify a 'stage'");
        }

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

    }

    public void start() {
        LOG.info("Check log '" + System.getProperty("user.home") + System.getProperty("file.separator") + ".hms-mirror/logs/hms-mirror.log'" +
                " for progress.");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        Config config = null;
        try {
            File cfgFile = new File(configFile);
            if (!cfgFile.exists()) {
                throw new RuntimeException("Missing configuration file: " + configFile);
            } else {
                System.out.println("Using Config: " + configFile);
            }
            String yamlCfgFile = FileUtils.readFileToString(cfgFile, Charset.forName("UTF-8"));
            config = mapper.readerFor(Config.class).readValue(yamlCfgFile);
        } catch (Throwable t) {
            t.printStackTrace();
        }

        Conversion conversion = new Conversion();

        ConnectionPools connPools = new ConnectionPools();
        connPools.addHiveServer2(Environment.LOWER, config.getCluster(Environment.LOWER).getHiveServer2());
        connPools.addHiveServer2(Environment.UPPER, config.getCluster(Environment.UPPER).getHiveServer2());
        connPools.init();

        config.getCluster(Environment.LOWER).setPools(connPools);
        config.getCluster(Environment.UPPER).setPools(connPools);

        LOG.info("Start Processing for databases: " + Arrays.toString((databases)));
        for (String database : databases) {
            switch (stage) {
                case METADATA:
                    LOG.info("Processing Database: " + database);
                    LOG.info("Building Lower Cluster");
                    try {
                        config.getCluster(Environment.LOWER).loadTables(conversion, database);
                        config.getCluster(Environment.LOWER).buildTransferSchema(conversion, database, config);
                        config.getCluster(Environment.LOWER).exportTransferSchema(conversion, database, config);

                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }

                    LOG.info("Building Upper Cluster");
                    try {
                        // Load Tables from the Upper Cluster.
                        // Use this to compare with incoming tables.
                        // NEED TO ADDRESS 'STAGE-1' here.
                        config.getCluster(Environment.UPPER).loadTables(conversion, database);
                    } catch (SQLException throwables) {
                        // Most likely means the db doesn't exist.
                        LOG.info(throwables.getMessage());
                    }
                    // CREATE DB ON UPPER CLUSTER.
                    if (config.getCluster(Environment.UPPER).importTransferSchema(conversion, database, config)) {
                        LOG.info("HMS Mirror for database: " + database + " complete.");
                    } else {
                        LOG.warn("HMS Mirror for database: " + database + " had issues.  Check logs");
                    }
                    break;
                case STORAGE:
                    // WIP
                    LOG.warn("Stage 2, WIP - Under development");
                    break;
            }
//        conversion.translate(config.getLowerCluster(),config.getUpperCluster(), database);
            LOG.info("DB: " + database + " complete.");
        }
        LOG.info("Done.");
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
