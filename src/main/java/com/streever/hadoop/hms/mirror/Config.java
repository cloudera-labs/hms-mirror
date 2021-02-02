package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.streever.hadoop.hms.Mirror;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;

public class Config {

    private static Logger LOG = LogManager.getLogger(Mirror.class);

    @JsonIgnore
    private ScheduledExecutorService metadataThreadPool;
    @JsonIgnore
    private ScheduledExecutorService storageThreadPool;

    private Stage stage = null;
    private boolean execute = Boolean.FALSE;
    private ReplicationStrategy replicationStrategy = ReplicationStrategy.SYNCHRONIZE;
    private boolean shareStorage = Boolean.FALSE;
    private boolean commitToUpper = Boolean.FALSE;


    @JsonIgnore // wip
    private String dbRegEx = null;
    @JsonIgnore
    private Pattern dbFilterPattern = null;
    private String[] databases = null;
    private String tblRegEx = null;

    private MetadataConfig metadata = new MetadataConfig();
    private StorageConfig storage = new StorageConfig();

    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();

    public boolean isExecute() {
        if (!execute) {
            LOG.debug("Dry-run: ON");
        }
        return execute;
    }

    public boolean isShareStorage() {
        return shareStorage;
    }

    public void setShareStorage(boolean shareStorage) {
        this.shareStorage = shareStorage;
    }

    public boolean isCommitToUpper() {
        return commitToUpper;
    }

    public void setCommitToUpper(boolean commitToUpper) {
        this.commitToUpper = commitToUpper;
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

    public String getDbRegEx() {
        return dbRegEx;
    }

    public void setDbRegEx(String dbRegEx) {
        this.dbRegEx = dbRegEx;
    }

    public String getTblRegEx() {
        return tblRegEx;
    }

    public void setTblRegEx(String tblRegEx) {
        this.tblRegEx = tblRegEx;
        if (this.tblRegEx != null)
            dbFilterPattern = Pattern.compile(tblRegEx);
        else
            dbFilterPattern = null;
    }

    public Pattern getDbFilterPattern() {
        return dbFilterPattern;
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

    public void setExecute(boolean execute) {
        this.execute = execute;
    }

    public ReplicationStrategy getReplicationStrategy() {
        return replicationStrategy;
    }

    public void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
        this.replicationStrategy = replicationStrategy;
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
        for (Environment environment : environmentSet) {
            Cluster cluster = clusters.get(environment);
            cluster.setEnvironment(environment);
        }
    }

}
