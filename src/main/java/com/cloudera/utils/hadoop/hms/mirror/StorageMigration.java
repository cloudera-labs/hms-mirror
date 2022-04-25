package com.cloudera.utils.hadoop.hms.mirror;

public class StorageMigration {

    private DataStrategy strategy = DataStrategy.SQL;

    public DataStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(DataStrategy strategy) {
        switch (strategy) {
            case SQL:
            case EXPORT_IMPORT:
            case HYBRID:
                this.strategy = strategy;
                break;
            default:
                throw new RuntimeException("Invalid strategy for STORAGE_MIGRATION");
        }
    }
}
