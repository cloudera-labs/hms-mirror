package com.streever.hadoop.hms.mirror;

import com.streever.hadoop.hms.Mirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Config {

    private static Logger LOG = LogManager.getLogger(Mirror.class);

    private ScheduledExecutorService metadataThreadPool;
    private ScheduledExecutorService storageThreadPool;

    private boolean dryrun = Boolean.FALSE;
    private Stage stage = null;
    private String[] databases = null;
    private String transferPrefix = "transfer_";
    private String exportBaseDirPrefix = "/apps/hive/warehouse/export_";
    private boolean overwriteTable = Boolean.TRUE;
    private MetadataConfig metadata = new MetadataConfig();
    private StorageConfig storage = new StorageConfig();

    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();

    public boolean isDryrun() {
        if (dryrun) {
            LOG.debug("Dry-run: ON");
        }
        return dryrun;
    }

    public ScheduledExecutorService getMetadataThreadPool() {
        if (metadataThreadPool == null) {
            metadataThreadPool = Executors.newScheduledThreadPool(getMetadata().getConcurrency());
        }
        return metadataThreadPool;
    }

    public ScheduledExecutorService getStorageThreadPool() {
        if (storageThreadPool == null) {
            storageThreadPool = Executors.newScheduledThreadPool(getStorage().getConcurrency());
        }
        return storageThreadPool;
    }

    public String[] getDatabases() {
        return databases;
    }

    public void setDatabases(String[] databases) {
        this.databases = databases;
    }

    public Stage getStage() {
        return stage;
    }

    public void setStage(Stage stage) {
        this.stage = stage;
    }

    public void setDryrun(boolean dryrun) {
        this.dryrun = dryrun;
    }

    public String getTransferPrefix() {
        return transferPrefix;
    }

    public void setTransferPrefix(String transferPrefix) {
        this.transferPrefix = transferPrefix;
    }

    public String getExportBaseDirPrefix() {
        return exportBaseDirPrefix;
    }

    public void setExportBaseDirPrefix(String exportBaseDirPrefix) {
        this.exportBaseDirPrefix = exportBaseDirPrefix;
    }

    public boolean isOverwriteTable() {
        return overwriteTable;
    }

    public void setOverwriteTable(boolean overwriteTable) {
        this.overwriteTable = overwriteTable;
    }

    public MetadataConfig getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataConfig metadata) {
        this.metadata = metadata;
    }

    public StorageConfig getStorage() {
        return storage;
    }

    public void setStorage(StorageConfig storage) {
        this.storage = storage;
    }

//    public Concurrency getConcurrency() {
//        return concurrency;
//    }
//
//    public void setConcurrency(Concurrency concurrency) {
//        this.concurrency = concurrency;
//    }

    //    public Integer getParallelism() {
//        return parallelism;
//    }
//
//    public void setParallelism(Integer parallelism) {
//        this.parallelism = parallelism;
//    }

    public Map<Environment, Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(Map<Environment, Cluster> clusters) {
        this.clusters = clusters;
    }

    public Cluster getCluster(Environment environment) {
        Cluster cluster = getClusters().get(environment);
        if (cluster.getEnvironment() == null) {
            cluster.setEnvironment(environment);
        }
        return cluster;
    }

    public void init() {
        // Link Cluster and it's Environment Type.
        Set<Environment> environmentSet = this.getClusters().keySet();
        for (Environment environment: environmentSet) {
            Cluster cluster = clusters.get(environment);
            cluster.setEnvironment(environment);
        }
    }
}
