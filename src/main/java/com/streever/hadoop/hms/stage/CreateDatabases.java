package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.Mirror;
import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.Conversion;
import com.streever.hadoop.hms.mirror.DBMirror;
import com.streever.hadoop.hms.mirror.Environment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.Set;

public class CreateDatabases implements Runnable {
    private static Logger LOG = LogManager.getLogger(CreateDatabases.class);

    private Config config = null;
    private Conversion conversion = null;
    // Flag use to determine is creating transition db in lower cluster
    // or target db in upper cluster.
    private boolean transition = Boolean.FALSE;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public CreateDatabases(Config config, Conversion conversion, Boolean transition) {
        this.config = config;
        this.conversion = conversion;
        this.transition = transition;
    }

    @Override
    public void run() {
        LOG.debug("Create Databases");
        Set<String> databases = conversion.getDatabases().keySet();
        for (String database : databases) {
            if (transition) {
                LOG.debug("Creating (LOWER) Transition Database: " + database);
                config.getCluster(Environment.LOWER).createDatabase(config, config.getTransferDbPrefix() + database);
            } else {
                LOG.debug("Creating (UPPER) Database: " + database);
                config.getCluster(Environment.UPPER).createDatabase(config, database);
            }
        }
        successful = Boolean.TRUE;
    }
}
