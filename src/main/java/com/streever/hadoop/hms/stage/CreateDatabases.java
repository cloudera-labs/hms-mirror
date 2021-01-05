package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.Environment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CreateDatabases implements Runnable {
    private static Logger LOG = LogManager.getLogger(CreateDatabases.class);

    private Config config = null;
    // Flag use to determine is creating transition db in lower cluster
    // or target db in upper cluster.
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public CreateDatabases(Config config) {
        this.config = config;
    }

    @Override
    public void run() {
        LOG.debug("Create Databases");

        for (String database : config.getDatabases()) {
            switch (config.getStage()) {
                case METADATA:
                    switch (config.getMetadata().getStrategy()) {
                        case EXPORT_IMPORT:
                            // Create transition in LOWER
                            config.getCluster(Environment.LOWER).createDatabase(config, config.getTransferPrefix() + database);
                        case DIRECT:
                            // Create target DB in UPPER
                            config.getCluster(Environment.UPPER).createDatabase(config, database);
                            break;
                    }
                    break;
                case STORAGE:
                    switch (config.getStorage().getStrategy()) {
                        case HYBRID:
                        case SQL:
                            // Create transition DB in UPPER
                            // 86 this.  Can't move tables between db's.  So we'll create a transition table in the
                            //    target db.
//                            config.getCluster(Environment.UPPER).createDatabase(config, config.getTransferDbPrefix() + database);
                        case EXPORT_IMPORT:
                            // Create target DB in UPPER
                            config.getCluster(Environment.UPPER).createDatabase(config, database);
                            break;
                        case DISTCP:
                            // WIP
                            break;
                    }
                    break;
            }
        }
        successful = Boolean.TRUE;
    }
}
