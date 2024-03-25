/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hive.config.DBStore;
import com.cloudera.utils.hive.config.QueryDefinitions;
import com.cloudera.utils.hms.mirror.Cluster;
import com.cloudera.utils.hms.mirror.Config;
import com.cloudera.utils.hms.mirror.Environment;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class QueryDefinitionsService {

    private final Config config;

    private final Map<Environment, QueryDefinitions> queryDefinitionsMap = new HashMap<>();

    public QueryDefinitionsService(Config config) {
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
                        String yamlConfigDefinition = IOUtils.toString(configURL, StandardCharsets.UTF_8);
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

}
