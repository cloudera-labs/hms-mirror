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

package com.cloudera.utils.hms.mirror.integration.end_to_end;

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.Pair;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.service.DomainService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.HMSMirrorAppService;
import com.cloudera.utils.hms.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static com.cloudera.utils.hms.mirror.MirrorConf.*;
import static org.junit.Assert.*;

@Slf4j
@Getter
@ActiveProfiles("no-cli")
public class E2EBaseTest {

    protected DomainService domainService;
    protected HMSMirrorAppService hmsMirrorAppService;
    protected ExecuteSessionService executeSessionService;
    //HMSMirrorAppService;

    protected HmsMirrorConfig getConfig() {
        return executeSessionService.getSession().getConfig();
    }

    protected ExecuteSessionService getExecuteSessionService() {
        return executeSessionService;
    }

    protected Conversion getConversion() {
        return executeSessionService.getSession().getConversion();
    }

    protected String[] getDatabasesFromTestDataFile(String testDataSet) {
        System.out.println("Test data file: " + testDataSet);
        Conversion conversion = domainService.deserializeConversion(testDataSet);
        String[] databases = null;

        databases = conversion.getDatabases().keySet().toArray(new String[0]);

        return databases;
    }

    protected String getDistcpLine(String outputDir, DBMirror[] dbMirrors, int dbInstance, Environment side,
                                   int distcpSource, int lineNum) {
        String rtn = null;
        String dbName = dbMirrors[dbInstance].getName();

        // Open the distcp file for the RIGHT.
        String distcpFile = outputDir + "/" + dbName + "_" + side.toString() + "_" + distcpSource + "_distcp_source.txt";
        File file = new File(distcpFile);
        if (!file.exists()) {
            fail("Distcp file doesn't exist: " + distcpFile);
        } else {
            try {
                List<String> lines = Files.readAllLines(file.toPath());
                rtn = lines.get(lineNum);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                fail("Error reading distcp file: " + distcpFile);
            }
        }
        return rtn;
    }

    protected long getCheckCode(MessageCode... messageCodes) {
        int check = 0;
        BitSet bitSet = new BitSet(150);
        long expected = 0;
        for (MessageCode messageCode : messageCodes) {
            bitSet.set(messageCode.ordinal());
        }
        long[] messageSet = bitSet.toLongArray();
        for (long messageBit : messageSet) {
            expected = expected | messageBit;
        }
        // Errors should be negative return code.
        return expected * -1;
    }

//    protected RunStatus getProgression() {
//        return HMSMirrorAppService.getRunStatus();
//    }

    protected DBMirror[] getResults(String outputDirBase, String sourceTestDataSet) {
        List<DBMirror> dbMirrorList = new ArrayList<>();
        System.out.println("Source Dataset: " + sourceTestDataSet);
        String[] databases = getDatabasesFromTestDataFile(sourceTestDataSet);
        for (String database : databases) {
            try {
                String resultsFileStr = outputDirBase + "/" + database + "_hms-mirror.yaml";
                URL resultURL = null;
                File resultsFile = new File(resultsFileStr);
                if (!resultsFile.exists())
                    throw new RuntimeException("Couldn't locate results file: " + resultsFileStr);
                resultURL = resultsFile.toURI().toURL();
                ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
                mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

                String yamlCfgFile = IOUtils.toString(resultURL, StandardCharsets.UTF_8);
                DBMirror dbMirror = mapper.readerFor(DBMirror.class).readValue(yamlCfgFile);
                dbMirrorList.add(dbMirror);
            } catch (UnrecognizedPropertyException upe) {
                throw new RuntimeException("\nThere may have been a breaking change in the configuration since the previous " +
                        "release. Review the note below and remove the 'Unrecognized field' from the configuration and try " +
                        "again.\n\n", upe);
            } catch (Throwable t) {
                // Look for yaml update errors.
                if (t.toString().contains("MismatchedInputException")) {
                    throw new RuntimeException("The format of the 'config' yaml file MAY HAVE CHANGED from the last release.  Please make a copy and run " +
                            "'-su|--setup' again to recreate in the new format", t);
                } else {
//                log.error(t);
                    throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
                }
            }
        }
        return dbMirrorList.toArray(new DBMirror[0]);
    }

    protected Long getReturnCode() {
        return hmsMirrorAppService.getReturnCode();
//        return executeSessionService.getSession().getRunStatus().getErrors().getReturnCode();
    }

    protected Long getWarningCode() {
        return hmsMirrorAppService.getWarningCode();
    }

    @Autowired
    public void setDomainService(DomainService domainService) {
        this.domainService = domainService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setHmsMirrorAppService(HMSMirrorAppService hmsMirrorAppService) {
        this.hmsMirrorAppService = hmsMirrorAppService;
    }

    protected void validateDBLocation(String database, Environment environment, String expectedLocation) {
        assertTrue("Database doesn't exist", getConversion().getDatabase(database) != null ? Boolean.TRUE : Boolean.FALSE);
        assertTrue("DB Environment doesn't exist", getConversion().getDatabase(database).getProperties().containsKey(environment));
        assertEquals("Location doesn't match", expectedLocation, getConversion().getDatabase(database).getProperties().get(environment).get(DB_LOCATION));
    }

    protected void validateDBManagedLocation(String database, Environment environment, String expectedLocation) {
        assertTrue("Database doesn't exist", getConversion().getDatabase(database) != null ? Boolean.TRUE : Boolean.FALSE);
        assertTrue("DB Environment doesn't exist", getConversion().getDatabase(database).getProperties().containsKey(environment));
        assertEquals("Managed Location doesn't match", expectedLocation, getConversion().getDatabase(database).getProperties().get(environment).get(DB_MANAGED_LOCATION));
    }

    protected void validatePartitionCount(String database, String tableName, Environment environment, int expectedIssueCount) {
        assertTrue("Database doesn't exist", getConversion().getDatabase(database) != null ? Boolean.TRUE : Boolean.FALSE);
        assertTrue("Table doesn't exist", getConversion().getDatabase(database).getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertEquals("Issue count doesn't match", expectedIssueCount, getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironmentTable(environment).getPartitions().size());
    }

    protected void validatePartitionLocation(String database, String tableName, Environment environment, String partitionSpec, String partitionLocation) {
        assertTrue("Database doesn't exist", getConversion().getDatabase(database) != null ? Boolean.TRUE : Boolean.FALSE);
        assertTrue("Table doesn't exist", getConversion().getDatabase(database).getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertTrue("Partition doesn't exist", getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironmentTable(environment).getPartitions().containsKey(partitionSpec));
        assertEquals("Location doesn't match", partitionLocation, getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironmentTable(environment).getPartitions().get(partitionSpec));
    }

    protected void validatePhase(String database, String tableName, PhaseState expectedPhaseState) {
        assertTrue("Database doesn't exist", getConversion().getDatabase(database) != null ? Boolean.TRUE : Boolean.FALSE);
        assertTrue("Table doesn't exist", getConversion().getDatabase(database)
                .getTableMirrors().containsKey(tableName));
        assertEquals("Phase State doesn't match", expectedPhaseState,
                getConversion().getDatabase(database)
                        .getTableMirrors().get(tableName)
                        .getPhaseState());
    }

    protected Boolean validateSqlPair(String database, Environment environment, String tableName, String description, String actionTest) {
        Boolean found = Boolean.FALSE;
        for (Pair pair : getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironmentTable(environment).getSql()) {
            if (pair.getDescription().trim().equals(description)) {
                assertEquals("Location doesn't match", actionTest, pair.getAction());
                found = Boolean.TRUE;
            }
        }
        return found;
    }

    protected void validateTableIssueCount(String database, String tableName, Environment environment, int expectedIssueCount) {
        assertTrue("Database doesn't exist", getConversion().getDatabase(database) != null ? Boolean.TRUE : Boolean.FALSE);
        assertTrue("Table doesn't exist", getConversion().getDatabase(database).getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertEquals("Issue count doesn't match", expectedIssueCount, getConversion().getDatabase(database)
                .getTableMirrors().get(tableName).getEnvironmentTable(environment).getIssues().size());
    }

    protected void validateTableLocation(String database, String tableName, Environment environment, String expectedLocation) {
        assertTrue("Database doesn't exist", getConversion().getDatabase(database) != null ? Boolean.TRUE : Boolean.FALSE);
        assertTrue("Table doesn't exist", getConversion().getDatabase(database).getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", getConversion().getDatabase(database).getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertEquals("Location doesn't match", expectedLocation,
                TableUtils.getLocation(tableName, getConversion().getDatabase(database)
                        .getTableMirrors().get(tableName).getEnvironmentTable(environment)
                        .getDefinition()));
    }
}
