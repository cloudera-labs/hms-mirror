package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HybridAcidDowngradeInPlaceDataStrategy extends DataStrategyBase implements DataStrategy  {

    private static final Logger LOG = LogManager.getLogger(HybridAcidDowngradeInPlaceDataStrategy.class);

    @Override
    public Boolean execute() {
        Boolean rtn = Boolean.TRUE;
        /*
        Check environment is Hive 3.
            if not, need to do SQLACIDInplaceDowngrade.
        If table is not partitioned
            go to export import downgrade inplace
        else if partitions <= hybrid.exportImportPartitionLimit
            go to export import downgrade inplace
        else if partitions <= hybrid.sqlPartitionLimit
            go to sql downgrade inplace
        else
            too many partitions.
         */
        if (config.getCluster(Environment.LEFT).getLegacyHive()) {
            DataStrategy dsSADI = DataStrategyEnum.SQL_ACID_DOWNGRADE_INPLACE.getDataStrategy();
            dsSADI.setTableMirror(tableMirror);
            dsSADI.setDBMirror(dbMirror);
            dsSADI.setConfig(config);
            rtn = dsSADI.execute();// doSQLACIDDowngradeInplace();
        } else {
            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            if (let.getPartitioned()) {
                // Partitions less than export limit or export limit set to 0 (or less), which means ignore.
                if (let.getPartitions().size() < config.getHybrid().getExportImportPartitionLimit() ||
                        config.getHybrid().getExportImportPartitionLimit() <= 0) {
                    DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE.getDataStrategy();
                    dsEI.setTableMirror(tableMirror);
                    dsEI.setDBMirror(dbMirror);
                    dsEI.setConfig(config);
                    rtn = dsEI.execute();// doEXPORTIMPORTACIDInplaceDowngrade();
                } else {
                    DataStrategy dsSADI = DataStrategyEnum.SQL_ACID_DOWNGRADE_INPLACE.getDataStrategy();
                    dsSADI.setTableMirror(tableMirror);
                    dsSADI.setDBMirror(dbMirror);
                    dsSADI.setConfig(config);
                    rtn = dsSADI.execute();// doSQLACIDDowngradeInplace();
                }
            } else {
                // Go with EXPORT_IMPORT
                DataStrategy dsEI = DataStrategyEnum.EXPORT_IMPORT_ACID_DOWNGRADE_INPLACE.getDataStrategy();
                dsEI.setTableMirror(tableMirror);
                dsEI.setDBMirror(dbMirror);
                dsEI.setConfig(config);
                rtn = dsEI.execute();// doEXPORTIMPORTACIDInplaceDowngrade();
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
