package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.Cluster;
import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.ConnectionPools;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.IOUtils;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;

import static com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum.STORAGE_MIGRATION;

public class Context {
    private static final Context instance = new Context();
    private List<String> supportFileSystems = new ArrayList<String>(Arrays.asList(
            "hdfs", "ofs", "s3", "s3a", "s3n", "wasb", "adls", "gf"
    ));
    private Config config = null;
    private Boolean initializing = Boolean.FALSE;
    private ConnectionPools connectionPools = null;

    private Map<Environment, QueryDefinitions> queryDefinitionsMap = new HashMap<>();

    private Context() {
    }

    public Boolean getInitializing() {
        return initializing;
    }

    public void setInitializing(Boolean initializing) {
        this.initializing = initializing;
    }

    public static Context getInstance() {
        return instance;
    }

    public Config getConfig() {
        return config;
    }

    public Boolean loadPartitionMetadata() {
        if (config.getEvaluatePartitionLocation() ||
                (config.getDataStrategy() == STORAGE_MIGRATION && config.getTransfer().getStorageMigration().isDistcp())) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public QueryDefinitions getQueryDefinitions(Environment environment) {
        QueryDefinitions queryDefinitions = queryDefinitionsMap.get(environment);
        if (queryDefinitions == null) {
            Cluster cluster = config.getCluster(environment);
            DBStore metastoreDirect = cluster.getMetastoreDirect();
            if (metastoreDirect != null) {
                DBStore.DB_TYPE dbType = metastoreDirect.getType();

                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

                try {
                    String dbQueryDefReference = "/" + dbType.toString() + "/metastore.yaml";
                    try {
                        URL configURL = this.getClass().getResource(dbQueryDefReference);
                        if (configURL == null) {
                            throw new RuntimeException("Can't build URL for Resource: " +
                                    dbQueryDefReference);
                        }
                        String yamlConfigDefinition = IOUtils.toString(configURL, Charset.forName("UTF-8"));
                        queryDefinitions = mapper.readerFor(QueryDefinitions.class).readValue(yamlConfigDefinition);
                        queryDefinitionsMap.put(environment, queryDefinitions);
                    } catch (Exception e) {
                        throw new RuntimeException("Missing resource file: " +
                                dbQueryDefReference, e);
                    }

                } catch (Exception e) {
                    throw new RuntimeException("Issue getting configs", e);
                }
            }
        }
        return queryDefinitions;
    }

    public ConnectionPools getConnectionPools() {
        return connectionPools;
    }

    public void setConnectionPools(ConnectionPools connectionPools) {
        this.connectionPools = connectionPools;
    }

    public List<String> getSupportFileSystems() {
        return supportFileSystems;
    }
}
