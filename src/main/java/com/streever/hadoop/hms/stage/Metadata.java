package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.Mirror;
import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.DBMirror;
import com.streever.hadoop.hms.mirror.Environment;
import com.streever.hadoop.hms.mirror.TableMirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.Date;

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
        Date start = new Date();
        LOG.info("METADATA: Migrating " + dbMirror.getDatabase() + "." + tblMirror.getName());
        /*
        Method supporting a transfer Schema.  Had issues with this on EMR and the EXPORT process.
        Could get the EXPORT the right permissions to write to S3, so the export would fail.
//        config.getCluster(Environment.LOWER).buildTransferTableSchema(config, dbMirror, tblMirror);
//        config.getCluster(Environment.LOWER).exportTransferSchema(config, dbMirror, tblMirror);
//        config.getCluster(Environment.UPPER).importTransferSchema(config, dbMirror, tblMirror);
         */
        // This method will skip the EXPORT transition step and build the schema from
        // the SOURCE table def.
        config.getCluster(Environment.UPPER).importSchema(config, dbMirror, tblMirror);
        successful = Boolean.TRUE;
        Date end = new Date();
        LOG.info("METADATA: Migration complete for " + dbMirror.getDatabase() + "." + tblMirror.getName() + " in " +
                Long.toString(end.getTime() - start.getTime()) + "ms");
    }
}
