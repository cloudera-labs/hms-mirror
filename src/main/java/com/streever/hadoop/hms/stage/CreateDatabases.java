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
                switch (config.getStage()) {
                    case METADATA:
                        switch (config.getMetadata().getStrategy()) {
                            case EXPORT_IMPORT:
                                // Create transition in LEFT
                                config.getCluster(Environment.LEFT).createDatabase(config, config.getMetadata().getTransferPrefix() + database);
                            case DIRECT:
                            case DISTCP:
                                // Create target DB in RIGHT
                                config.getCluster(Environment.RIGHT).createDatabase(config, config.getResolvedDB(database));
                                break;
                        }
                        break;
                    case STORAGE:
                        switch (config.getStorage().getStrategy()) {
                            case HYBRID:
                            case SQL:
                                // Create transition DB in RIGHT
                                // 86 this.  Can't move tables between db's.  So we'll create a transition table in the
                                //    target db.
//                            config.getCluster(Environment.RIGHT).createDatabase(config, config.getTransferDbPrefix() + database);
                            case EXPORT_IMPORT:
                                // Create target DB in RIGHT
                                config.getCluster(Environment.RIGHT).createDatabase(config, config.getResolvedDB(database));
                                break;
                            case DISTCP:
                                // TODO: WIP
                                break;
                        }
                        break;
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
