package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Properties;

public class HiveServer2Config {
    private String uri = null;
    private Properties connectionProperties;
    private String jarFile = null;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Properties getConnectionProperties() {
        if (connectionProperties == null) {
            setConnectionProperties(new Properties());
        }
        return connectionProperties;
    }

    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
        // Don't restrict max connections to the default.  Allow this to
        // be controlled by the 'concurrency' value of the 'transfer'.
        String maxConnections = connectionProperties.getProperty("maxTotal", "-1");
        connectionProperties.setProperty("maxTotal", maxConnections);
        // Default to 5 secs for timeout.
        String maxWait = connectionProperties.getProperty("maxWaitMillis", "5000");
        connectionProperties.setProperty("maxWaitMillis", maxWait);
    }

    public String getJarFile() {
        return jarFile;
    }

    public void setJarFile(String jarFile) {
        this.jarFile = jarFile;
    }

    @JsonIgnore
    public Boolean isValidUri() {
        Boolean rtn = Boolean.TRUE;
        if (getUri() == null || !getUri().startsWith("jdbc:hive2://")) {
            rtn = Boolean.FALSE;
        }
        return rtn;
    }

    @JsonIgnore
    public Boolean isKerberosConnection() {
        if (getUri().contains("principal")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    @JsonIgnore
    public Boolean isZooKeeperConnection() {
        if (getUri().contains("serviceDiscoveryMode=zooKeeper")) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }
}
