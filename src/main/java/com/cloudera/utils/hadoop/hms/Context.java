package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Context {
    private static final Context instance = new Context();
    private List<String> supportFileSystems = new ArrayList<String>(Arrays.asList(
            "hdfs","ofs","s3","s3a","s3n","wasb","adls","gf"
    ));
    private Config config = null;

    private Context() {};

    public static Context getInstance() {
        return instance;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public List<String> getSupportFileSystems() {
        return supportFileSystems;
    }
}
