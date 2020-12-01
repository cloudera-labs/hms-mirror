package com.streever.hadoop.hms.mirror;

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
        return connectionProperties;
    }

    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    public String getJarFile() {
        return jarFile;
    }

    public void setJarFile(String jarFile) {
        this.jarFile = jarFile;
    }
}
