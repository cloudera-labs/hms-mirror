package com.streever.hadoop.hms.mirror;

import com.streever.hadoop.shell.commands.Env;

import java.util.*;

public class Config {

    /*

     */
    private String transferDbPrefix = "transfer_";
    private String exportBaseDirPrefix = "/apps/hive/warehouse/export_";
    private boolean overwriteTable = Boolean.TRUE;

    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();
//    private Cluster lowerCluster = null;
//    private Cluster upperCluster = null;

    public String getTransferDbPrefix() {
        return transferDbPrefix;
    }

    public void setTransferDbPrefix(String transferDbPrefix) {
        this.transferDbPrefix = transferDbPrefix;
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
