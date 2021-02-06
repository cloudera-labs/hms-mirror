package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.Environment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class CreateDatabases implements Callable<ReturnStatus> {
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
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();
        LOG.debug("Create Databases");
        try {
            for (String database : config.getDatabases()) {
                switch (config.getDataStrategy()) {
                    case HYBRID:
                    case EXPORT_IMPORT:
                        config.getCluster(Environment.LEFT).createDatabase(config, config.getTransfer().getTransferPrefix() + database);
                        config.getCluster(Environment.RIGHT).createDatabase(config, config.getResolvedDB(database));
                        break;
                    case SQL:
                        config.getCluster(Environment.RIGHT).createDatabase(config, config.getTransfer().getTransferPrefix() + database);
                    case SCHEMA_ONLY:
                    case COMMON:
                    case LINKED:
                    case INTERMEDIATE:
                        config.getCluster(Environment.RIGHT).createDatabase(config, config.getResolvedDB(database));
                }

            }
            successful = Boolean.TRUE;
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (Exception t) {
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(t);
        }
        return rtn;
    }
}
