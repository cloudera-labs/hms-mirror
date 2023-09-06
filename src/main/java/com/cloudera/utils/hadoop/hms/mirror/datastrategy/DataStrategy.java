package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.DBMirror;
import com.cloudera.utils.hadoop.hms.mirror.TableMirror;

public interface DataStrategy {
    Boolean execute();

    Config getConfig();

    void setConfig(Config config);

    DBMirror getDBMirror();

    void setDBMirror(DBMirror dbMirror);

    TableMirror getTableMirror();

    void setTableMirror(TableMirror tableMirror);

    Boolean buildOutDefinition();
    Boolean buildOutSql();
}
