/*
 * Copyright (c) 2022-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.utils;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.*;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@ExtendWith(MockitoExtension.class)
@Slf4j
public abstract class TranslatorTestBase {

    @Mock
    ObjectMapper yamlMapper;

    @InjectMocks
    DomainService domainService;

    @InjectMocks
    ConfigService configService;

    @Mock
    EnvironmentService environmentService;

    @Mock
    PasswordService passwordService;

    @Mock
    CliEnvironment cliEnvironment;

    @InjectMocks
    ConnectionPoolService connectionPoolService;

    ExecuteSessionService executeSessionService;

    WarehouseService warehouseService;

    TranslatorService translatorService;

    Translator translator;
    HmsMirrorConfig config;

    @BeforeEach
    public void setup() throws IOException {
        log.info("Setting up Base");
        initializeConfig();
        initializeServices();
    }

    protected abstract void initializeConfig() throws IOException;

    protected void initializeServices() throws IOException {

        // ExecuteSessionService needs: ConfigService, CliEnvironment, ConnectionPoolService
        executeSessionService = new ExecuteSessionService(configService, cliEnvironment, connectionPoolService);

        // Warehouse needs: ExecuteSessionService,
        warehouseService = new WarehouseService(executeSessionService);

        // TranslatorService needs: ExecuteSessionService, WarehouseService
        translatorService = new TranslatorService(executeSessionService, warehouseService);

        ExecuteSession session = executeSessionService.createSession("test-session", config);
        executeSessionService.setSession(session);
        try {
            executeSessionService.startSession(1);
        } catch (SessionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
         * Deserializes a given resource file (YAML/JSON) into a {@link Translator} object.
         *
         * @param configResource path or classpath resource, must end with yaml/yml/json/jsn extension.
         * @return Translator instance
         * @throws IOException if reading the resource fails
         */
    public static Translator deserializeResource(String configResource) throws IOException {
        String extension = FilenameUtils.getExtension(configResource);
        ObjectMapper mapper;

        if ("yaml".equalsIgnoreCase(extension) || "yml".equalsIgnoreCase(extension)) {
            mapper = new ObjectMapper(new YAMLFactory());
        } else if ("json".equalsIgnoreCase(extension) || "jsn".equalsIgnoreCase(extension)) {
            mapper = new ObjectMapper(new JsonFactory());
        } else {
            throw new IllegalArgumentException(configResource + ": can't determine type by extension. Require one of: ['yaml','yml','json','jsn']");
        }

        // Try to load as a resource from the classpath
        URL configURL = TranslatorTestBase.class.getResource(configResource);
        if (configURL == null) {
            // Try as a file from local filesystem
            configURL = new URL("file", null, configResource);
        }

        if (configURL == null) {
            throw new IOException("Couldn't locate 'Serialized Record File': " + configResource);
        }

        String configDefinition = IOUtils.toString(configURL, StandardCharsets.UTF_8);
        return mapper.readerFor(Translator.class).readValue(configDefinition);
    }
}