package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.Mirror;
import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.DBMirror;
import com.streever.hadoop.hms.mirror.Environment;
import com.streever.hadoop.hms.mirror.TableMirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;

public class Metadata implements Runnable {
    private static Logger LOG = LogManager.getLogger(Metadata.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public Metadata(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }

    @Override
    public void run() {
        LOG.info("Metadata: " + dbMirror.getDatabase() + "." + tblMirror.getName());
        try {
            config.getCluster(Environment.LOWER).getTableDefinition(dbMirror.getDatabase(), tblMirror);
            config.getCluster(Environment.LOWER).buildTransferTableSchema(config, dbMirror, tblMirror);
            config.getCluster(Environment.LOWER).exportTransferSchema(config, dbMirror, tblMirror);
            config.getCluster(Environment.UPPER).importTransferSchema(config, dbMirror, tblMirror);
            successful = Boolean.TRUE;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            successful = Boolean.FALSE;
        }
    }
}
