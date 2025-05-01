package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.Translator;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.utils.ConfigTest;
import com.cloudera.utils.hms.mirror.utils.TranslatorTestBase;
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
public class ServiceTestBase {

    static final String TRANSLATOR_CONFIG = "/translator/testcase_01.yaml";
    static final String DEFAULT_CONFIG = "/config/default_01.yaml";

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

    protected void initializeConfig() throws IOException {
        translator = deserializeResource(TRANSLATOR_CONFIG);

        config = ConfigTest.deserializeResource(DEFAULT_CONFIG);
        config.setTranslator(translator);
        config.setLoadTestDataFile("something.yaml");
    }

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

    // TODO: Move to TranslatorTest
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
