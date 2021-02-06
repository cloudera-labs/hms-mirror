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
    private ScheduledExecutorService transferThreadPool;
//    @JsonIgnore
//    private ScheduledExecutorService metadataThreadPool;
//    @JsonIgnore
//    private ScheduledExecutorService storageThreadPool;

    private DataStrategy dataStrategy = DataStrategy.SCHEMA_ONLY;
    private HybridConfig hybrid = new HybridConfig();
    private Boolean migrateACID = Boolean.FALSE;

//    private Stage stage = null;
    private boolean execute = Boolean.FALSE;
    /*
    Used when a schema is transferred and has 'purge' properties for the table.
    When this is 'true', we'll remove the 'purge' option.
    This is helpful for datasets that are in DR, where the table doesn't
    control the Filesystem and we don't want to mess that up.
     */
    private boolean readOnly = Boolean.FALSE;

//    private ReplicationStrategy replicationStrategy = ReplicationStrategy.SYNCHRONIZE;
//    private boolean shareStorage = Boolean.FALSE;
//    private boolean commitToUpper = Boolean.FALSE;
    private Acceptance acceptance = new Acceptance();

    @JsonIgnore // wip
    private String dbRegEx = null;
    @JsonIgnore
    private Pattern dbFilterPattern = null;
    /*
   Prefix the DB with this to create an alternate db.
   Good for testing.

   Should leave null for like replication.
    */
    private String dbPrefix = null;

    private String[] databases = null;
    private String tblRegEx = null;

    private TransferConfig transfer = new TransferConfig();
//    private MetadataConfig metadata = new MetadataConfig();
//    private StorageConfig storage = new StorageConfig();

    private Map<Environment, Cluster> clusters = new TreeMap<Environment, Cluster>();

    public Boolean getMigrateACID() {
        return migrateACID;
    }

    public void setMigrateACID(Boolean migrateACID) {
        this.migrateACID = migrateACID;
    }

    public DataStrategy getDataStrategy() {
        return dataStrategy;
    }

    public void setDataStrategy(DataStrategy dataStrategy) {
        this.dataStrategy = dataStrategy;
    }

    public HybridConfig getHybrid() {
        return hybrid;
    }

    public void setHybrid(HybridConfig hybrid) {
        this.hybrid = hybrid;
    }

    public boolean isExecute() {
        if (!execute) {
            LOG.debug("Dry-run: ON");
        }
        return execute;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public Acceptance getAcceptance() {
        return acceptance;
    }

    public void setAcceptance(Acceptance acceptance) {
        this.acceptance = acceptance;
    }

    public String getDbPrefix() {
        return dbPrefix;
    }

    public void setDbPrefix(String dbPrefix) {
        this.dbPrefix = dbPrefix;
    }

    public String getResolvedDB(String database) {
        String rtn = null;
        rtn = (dbPrefix != null? dbPrefix + database: database);
        return rtn;
    }

//    public boolean isShareStorage() {
//        return shareStorage;
//    }
//
//    public void setShareStorage(boolean shareStorage) {
//        this.shareStorage = shareStorage;
//    }
//
//    public boolean isCommitToUpper() {
//        return commitToUpper;
//    }
//
//    public void setCommitToUpper(boolean commitToUpper) {
//        this.commitToUpper = commitToUpper;
//    }

    public ScheduledExecutorService getTransferThreadPool() {
        if (transferThreadPool == null) {
            transferThreadPool = Executors.newScheduledThreadPool(getTransfer().getConcurrency());
        }
        return transferThreadPool;
    }

//    public ScheduledExecutorService getMetadataThreadPool() {
//        if (metadataThreadPool == null) {
//            metadataThreadPool = Executors.newScheduledThreadPool(getMetadata().getConcurrency());
//        }
//        return metadataThreadPool;
//    }
//
//    public ScheduledExecutorService getStorageThreadPool() {
//        if (storageThreadPool == null) {
//            storageThreadPool = Executors.newScheduledThreadPool(getStorage().getConcurrency());
//        }
//        return storageThreadPool;
//    }

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

//    public Stage getStage() {
//        return stage;
//    }
//
//    public void setStage(Stage stage) {
//        this.stage = stage;
//    }

    public void setExecute(boolean execute) {
        this.execute = execute;
    }

//    public ReplicationStrategy getReplicationStrategy() {
//        return replicationStrategy;
//    }
//
//    public void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
//        this.replicationStrategy = replicationStrategy;
//    }
//
//    public MetadataConfig getMetadata() {
//        return metadata;
//    }
//
//    public void setMetadata(MetadataConfig metadata) {
//        this.metadata = metadata;
//    }
//
//    public StorageConfig getStorage() {
//        return storage;
//    }
//
//    public void setStorage(StorageConfig storage) {
//        this.storage = storage;
//    }

    public TransferConfig getTransfer() {
        return transfer;
    }

    public void setTransfer(TransferConfig transfer) {
        this.transfer = transfer;
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
