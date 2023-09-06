package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class AcidDataStrategy extends DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(AcidDataStrategy.class);
    @Override
    public Boolean execute() {
        return null;
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
