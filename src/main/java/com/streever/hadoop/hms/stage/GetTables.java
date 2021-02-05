package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.DBMirror;
import com.streever.hadoop.hms.mirror.Environment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class GetTables implements Callable<ReturnStatus> {
    private static Logger LOG = LogManager.getLogger(GetTables.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private boolean successful = Boolean.FALSE;

    public Config getConfig() {
        return config;
    }

    public DBMirror getDbMirror() {
        return dbMirror;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public GetTables(Config config, DBMirror dbMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
    }

    @Override
    public ReturnStatus call() {
        ReturnStatus rtn = new ReturnStatus();
        LOG.debug("Getting tables for: " +dbMirror.getName());
        try {
            config.getCluster(Environment.LEFT).getTables(config, dbMirror);
            successful = Boolean.TRUE;
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } catch (SQLException throwables) {
            successful = Boolean.FALSE;
            rtn.setStatus(ReturnStatus.Status.ERROR);
            rtn.setException(throwables);
        }
        return rtn;
    }
}
