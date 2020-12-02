package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.DBMirror;
import com.streever.hadoop.hms.mirror.Environment;
import com.streever.hadoop.hms.mirror.TableMirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;

public class GetTableMetadata implements Runnable {
    private static Logger LOG = LogManager.getLogger(GetTableMetadata.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
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

    public GetTableMetadata(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }

    @Override
    public void run() {
        LOG.info(dbMirror.getDatabase() + ":Get Tables");
        try {
            config.getCluster(Environment.LOWER).getTableDefinition(dbMirror.getDatabase(), tblMirror);
            config.getCluster(Environment.UPPER).getTableDefinition(dbMirror.getDatabase(), tblMirror);
            successful = Boolean.TRUE;
        } catch (SQLException throwables) {
            successful = Boolean.FALSE;
        }
    }
}
