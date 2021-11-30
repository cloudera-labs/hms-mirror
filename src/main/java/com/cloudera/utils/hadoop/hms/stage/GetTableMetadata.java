package com.cloudera.utils.hadoop.hms.stage;

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.DBMirror;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.TableMirror;
import com.cloudera.utils.hadoop.hms.mirror.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class GetTableMetadata implements Callable<ReturnStatus> {
    private static Logger LOG = LogManager.getLogger(GetTableMetadata.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.TRUE;

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
    public ReturnStatus call() {
        return doit();
    }

    public ReturnStatus doit() {
        ReturnStatus rtn = new ReturnStatus();
        LOG.debug("Getting table definition for: " + dbMirror.getName() + "." + tblMirror.getName());
        try {
            config.getCluster(Environment.LEFT).getTableDefinition(config, dbMirror.getName(), tblMirror);
            switch (config.getDataStrategy()) {
                case DUMP:
                    successful = Boolean.TRUE;
                    break;
                default:
                    config.getCluster(Environment.RIGHT).getTableDefinition(config, config.getResolvedDB(dbMirror.getName()), tblMirror);
            }
        } catch (SQLException throwables) {
            successful = Boolean.FALSE;
        }
        if (successful) {
            rtn.setStatus(ReturnStatus.Status.SUCCESS);
        } else {
            rtn.setStatus(ReturnStatus.Status.ERROR);
        }
        LOG.debug("Completed table definition for: " + dbMirror.getName() + "." + tblMirror.getName());
        return rtn;
    }
}
