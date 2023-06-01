/*
 * Copyright (c) 2022. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.Config;
import com.cloudera.utils.hadoop.hms.mirror.Environment;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class DataState {

    private static DataState instance = null;

    protected String configuration = null;
//    protected Boolean dataCreated = Boolean.FALSE;
    protected Map<String, Map<String, Boolean>> dataCreated = new TreeMap<>();
//    protected Map<String, Boolean> dataCreated = new TreeMap<String, Boolean>();
    private Boolean skipAdditionDataCreation = Boolean.FALSE;

    protected Boolean execute = Boolean.FALSE;
    protected Boolean cleanUp = Boolean.TRUE;
    protected Boolean populate = Boolean.TRUE;

    protected String working_db = null;
    protected String table_filter = null;

    private final String unique = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

    private DataState() {

    }

    public static DataState getInstance() {
        if (instance == null)
            instance = new DataState();
        return instance;
    }

    public String getUnique() {
        return unique;
    }

    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(String configuration) throws IOException {
        this.configuration = System.getProperty("user.home") +
                "/.hms-mirror/cfg/" + configuration;
        File cfgFile = new File(this.configuration);
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String yamlCfgFile = FileUtils.readFileToString(cfgFile, StandardCharsets.UTF_8);
        Config cfg = mapper.readerFor(Config.class).readValue(yamlCfgFile);
        Context.getInstance().setConfig(cfg);
    }

    public String getTable_filter() {
        return table_filter;
    }

    public void setTable_filter(String table_filter) {
        this.table_filter = table_filter;
    }

    protected String getTestDbName() {
        return "z_hms_mirror_testdb_" + unique;
    }

    public String getWorking_db() {
        if (working_db == null)
            working_db = getTestDbName();
        return working_db;
    }

    public void setWorking_db(String working_db) {
        this.working_db = working_db;
    }

    public Boolean isCleanUp() {
        return cleanUp;
    }

    public Boolean getSkipAdditionDataCreation() {
        return skipAdditionDataCreation;
    }

    public void setSkipAdditionDataCreation(Boolean skipAdditionDataCreation) {
        this.skipAdditionDataCreation = skipAdditionDataCreation;
    }

    public Boolean getPopulate() {
        return populate;
    }

    public void setPopulate(Boolean populate) {
        this.populate = populate;
    }

    public Boolean isDataCreated(String dataset) {
        Boolean rtn = Boolean.FALSE;
        if (!skipAdditionDataCreation) {
            Config cfg = Context.getInstance().getConfig();
            String namespace = cfg.getCluster(Environment.LEFT).getHcfsNamespace();
            Map<String, Boolean> nsCreatedDataset = dataCreated.get(namespace);
            if (nsCreatedDataset == null) {
                nsCreatedDataset = new TreeMap<String, Boolean>();
                dataCreated.put(namespace, nsCreatedDataset);
            }
            if (nsCreatedDataset.containsKey(dataset)) {
                rtn = Boolean.TRUE;
            }
        } else {
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public Boolean isExecute() {
        return execute;
    }

    public void setCleanUp(Boolean cleanUp) {
        this.cleanUp = cleanUp;
    }

    public void setDataCreated(String dataset, Boolean dataCreatedFlag) {
        Config cfg = Context.getInstance().getConfig();
        String namespace = cfg.getCluster(Environment.LEFT).getHcfsNamespace();
        Map<String, Boolean> nsCreatedDataset = dataCreated.get(namespace);
        if (nsCreatedDataset == null) {
            nsCreatedDataset = new TreeMap<String, Boolean>();
            dataCreated.put(namespace, nsCreatedDataset);
        }
        nsCreatedDataset.put(dataset, dataCreatedFlag);
    }

    public void setExecute(Boolean execute) {
        this.execute = execute;
    }
}
