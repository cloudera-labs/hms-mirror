package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.Mirror;
import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.DBMirror;
import com.streever.hadoop.hms.mirror.Environment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;

public class GetTables implements Runnable {
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
    public void run() {
        LOG.debug("Getting table for: " +dbMirror.getDatabase());
        try {
            config.getCluster(Environment.LOWER).getTables(dbMirror);
            successful = Boolean.TRUE;
        } catch (SQLException throwables) {
            successful = Boolean.FALSE;
        }
    }
}
