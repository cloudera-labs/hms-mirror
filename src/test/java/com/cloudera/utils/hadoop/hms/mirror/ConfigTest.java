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

package com.cloudera.utils.hadoop.hms.mirror;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ConfigTest {

    public static Config deserializeResource(String configResource) throws IOException {
        Config config = null;
        String extension = FilenameUtils.getExtension(configResource);
        ObjectMapper mapper = null;
        if ("yaml".equalsIgnoreCase(extension) || "yml".equalsIgnoreCase(extension)) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else if ("json".equalsIgnoreCase(extension) || "jsn".equalsIgnoreCase(extension)) {
            mapper = new ObjectMapper(new JsonFactory());
        } else {
            throw new RuntimeException(configResource + ": can't determine type by extension.  Require one of: ['yaml',yml,'json','jsn']");
        }

        // Try as a Resource (in classpath)
        URL configURL = mapper.getClass().getResource(configResource);
        if (configURL != null) {
            // Convert to String.
            String configDefinition = IOUtils.toString(configURL, StandardCharsets.UTF_8);
            config = mapper.readerFor(Config.class).readValue(configDefinition);
        } else {
            // Try on Local FileSystem.
            configURL = new URL("file", null, configResource);
            if (configURL != null) {
                String configDefinition = IOUtils.toString(configURL, StandardCharsets.UTF_8);
                config = mapper.readerFor(Config.class).readValue(configDefinition);
            } else {
                throw new RuntimeException("Couldn't locate 'Serialized Record File': " + configResource);
            }
        }

        return config;
    }

}