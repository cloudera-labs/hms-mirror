package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HybridDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(HybridDataStrategy.class);
    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.FALSE;

        // Need to look at table.  ACID tables go to doACID()
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);

        // Acid tables between legacy and non-legacy are forced to intermediate
        if (TableUtils.isACID(let) && config.legacyMigration()) {
            tableMirror.setStrategy(DataStrategyEnum.ACID);
            if (config.getMigrateACID().isOn()) {
                DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
                dsIt.setTableMirror(tableMirror);
                dsIt.setDBMirror(dbMirror);
                dsIt.setConfig(config);
                rtn = dsIt.execute();//doIntermediateTransfer();
            } else {
                let.addIssue(TableUtils.ACID_NOT_ON);
                rtn = Boolean.FALSE;
            }
        } else {
            if (let.getPartitioned()) {
                if (let.getPartitions().size() > config.getHybrid().getExportImportPartitionLimit() &&
                        config.getHybrid().getExportImportPartitionLimit() > 0) {
                    // SQL
                    let.addIssue("The number of partitions: " + let.getPartitions().size() + " exceeds the EXPORT_IMPORT " +
                            "partition limit (hybrid->exportImportPartitionLimit) of " + config.getHybrid().getExportImportPartitionLimit() +
                            ".  Hence, the SQL method has been selected for the migration.");

                    tableMirror.setStrategy(DataStrategyEnum.SQL);
                    if (config.getTransfer().getIntermediateStorage() != null ||
                            config.getTransfer().getCommonStorage() != null) {
                        DataStrategy dsIt = DataStrategyEnum.INTERMEDIATE.getDataStrategy();
                        dsIt.setTableMirror(tableMirror);
                        dsIt.setDBMirror(dbMirror);
                        dsIt.setConfig(config);
                        rtn = dsIt.execute();//doIntermediateTransfer();
                    } else {
                        DataStrategy dsSQL = DataStrategyEnum.SQL.getDataStrategy();
                        dsSQL.setTableMirror(tableMirror);
                        dsSQL.setDBMirror(dbMirror);
                        dsSQL.setConfig(config);
                        rtn = dsSQL.execute();// doSQL();
                    }
                } else {
                    // EXPORT
                    tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                    DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT.getDataStrategy();
                    dsEI.setTableMirror(tableMirror);
                    dsEI.setDBMirror(dbMirror);
                    dsEI.setConfig(config);
                    rtn = dsEI.execute();// doExportImport();
                }
            } else {
                // EXPORT
                tableMirror.setStrategy(DataStrategyEnum.EXPORT_IMPORT);
                DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT.getDataStrategy();
                dsEI.setTableMirror(tableMirror);
                dsEI.setDBMirror(dbMirror);
                dsEI.setConfig(config);
                rtn = dsEI.execute();// doExportImport();
            }
        }
        return rtn;

    }

    @Override
    public Boolean buildOutDefinition() {
        return null;
    }

    @Override
    public Boolean buildOutSql() {
        return null;
    }
}
