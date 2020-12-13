package com.streever.hadoop.hms.stage;

import com.streever.hadoop.hms.mirror.Config;
import com.streever.hadoop.hms.mirror.DBMirror;
import com.streever.hadoop.hms.mirror.Environment;
import com.streever.hadoop.hms.mirror.TableMirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Date;

public class Storage implements Runnable {
    private static Logger LOG = LogManager.getLogger(Storage.class);

    private Config config = null;
    private DBMirror dbMirror = null;
    private TableMirror tblMirror = null;
    private boolean successful = Boolean.FALSE;

    public boolean isSuccessful() {
        return successful;
    }

    public Storage(Config config, DBMirror dbMirror, TableMirror tblMirror) {
        this.config = config;
        this.dbMirror = dbMirror;
        this.tblMirror = tblMirror;
    }

    @Override
    public void run() {
        Date start = new Date();
        LOG.info("STORAGE: Migrating " + dbMirror.getDatabase() + "." + tblMirror.getName());

        // Set Database to Transfer DB.

        switch (config.getStorage().getStrategy()) {
            case SQL:
                // Build the Transfer table (with prefix)
                config.getCluster(Environment.UPPER).buildUpperTransferTable(config, dbMirror, tblMirror);
                // Like the DIRECT METASTORE method.
                // Create table in new cluster.
                    // Associate data to original cluster data.
                config.getCluster(Environment.UPPER).buildUpperSchemaUsingLowerData(config, dbMirror, tblMirror);

                // Create a Transition table that is in the target db.

                // sql to move data.
                config.getCluster(Environment.UPPER).sqlDataTransfer(config, dbMirror, tblMirror);

                // sql to drop (if exists) target table

                // rename move table to target table.

                break;
            case EXPORT_IMPORT:
                String database = config.getTransferPrefix() + dbMirror.getDatabase();

                break;
            case HYBRID:
                break;
            case DISTCP:
                break;
        }
        /*
         */
        successful = Boolean.TRUE;
        Date end = new Date();
        LOG.info("METADATA: Migration complete for " + dbMirror.getDatabase() + "." + tblMirror.getName() + " in " +
                Long.toString(end.getTime() - start.getTime()) + "ms");
    }
}
