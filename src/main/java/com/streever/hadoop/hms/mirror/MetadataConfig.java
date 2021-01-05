package com.streever.hadoop.hms.mirror;

public class MetadataConfig {
    private int concurrency = 4;
    private Strategy strategy = Strategy.DIRECT;

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public String toString() {
        return "MetadataConfig{" +
                "concurrency=" + concurrency +
                ", strategy=" + strategy +
                '}';
    }
}
