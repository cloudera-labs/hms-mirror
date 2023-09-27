/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EndToEndBase {
    protected static String homedir = System.getProperty("user.home");
    protected static String separator = System.getProperty("file.separator");

    private String outputDirBase = null;
    private Pattern filePattern = Pattern.compile("[^/]+(?=/$|$)");

    protected String getOutputDirBase() {
        if (outputDirBase == null) {
            outputDirBase = homedir + separator + "hms-mirror-reports" + separator +
                    DataState.getInstance().getUnique() + separator +
                    getClass().getSimpleName() + separator;
        }
        return outputDirBase;
    }

//    protected String getResultsFile(String testDataFile) {
//        // Extract the filePattern from the testDataFile with the filePattern pattern.
//        Matcher matcher = filePattern.matcher(testDataFile);
//        String resultsFile = matcher.find() ? matcher.group(0) : null;
//        resultsFile = resultsFile.substring(0, resultsFile.lastIndexOf("."));
//        return resultsFile + "_hms-mirror-results.yaml";
//    }

//    @Test
//    public void testResultFilePattern() {
//        String testDataFile = "/src/test/resources/com/cloudera/utils/hadoop/hms/mirror/EndToEndBaseTest.yaml";
//        String resultsFile = getResultsFile(testDataFile);
//        assertEquals("EndToEndBaseTest", resultsFile);
//    }

    protected String[] getDatabasesFromTestDataFile(String testDataSet) {
        System.out.println("Test data file: " + testDataSet);

        URL configURL = this.getClass().getResource(testDataSet);
        if (configURL == null) {

            File conversionFile = new File(testDataSet);
            if (!conversionFile.exists())
                throw new RuntimeException("Couldn't locate test data file: " + testDataSet);
            try {
                configURL = conversionFile.toURI().toURL();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        String yamlCfgFile = null;
        try {
            yamlCfgFile = IOUtils.toString(configURL, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Conversion conversion = null;
        try {
            conversion = mapper.readerFor(Conversion.class).readValue(yamlCfgFile);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        // Set Config Databases;
        String[] databases = conversion.getDatabases().keySet().toArray(new String[0]);
        return databases;
    }


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
//                LOG.error(t);
                    throw new RuntimeException("A configuration element is no longer valid, progress.  Please remove the element from the configuration yaml and try again.", t);
                }
            }
        }
        return dbMirrorList.toArray(new DBMirror[0]);
    }

    protected Boolean validateSqlPair(DBMirror dbMirror, Environment environment, String tableName, String description, String actionTest) {
        Boolean found = Boolean.FALSE;
        for (Pair pair : dbMirror.getTableMirrors().get(tableName).getEnvironmentTable(environment).getSql()) {
            if (pair.getDescription().trim().equals(description)) {
                assertEquals("Location doesn't match", actionTest, pair.getAction());
                found = Boolean.TRUE;
            }
        }
        return found;
    }

    protected void validateTableIssueCount(DBMirror dbMirror, String tableName, Environment environment, int expectedIssueCount) {
        assertTrue("Table doesn't exist", dbMirror.getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", dbMirror.getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertEquals("Issue count doesn't match", expectedIssueCount, dbMirror.getTableMirrors().get(tableName).getEnvironmentTable(environment).getIssues().size());
    }

    protected void validatePartitionCount(DBMirror dbMirror, String tableName, Environment environment, int expectedIssueCount) {
        assertTrue("Table doesn't exist", dbMirror.getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", dbMirror.getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertEquals("Issue count doesn't match", expectedIssueCount, dbMirror.getTableMirrors().get(tableName).getEnvironmentTable(environment).getPartitions().size());
    }


    protected void validatePhase(DBMirror dbMirror, String tableName, PhaseState expectedPhaseState) {
        assertEquals("Phase State doesn't match", expectedPhaseState, dbMirror.getTableMirrors().get(tableName).getPhaseState());
    }

    protected void validateTableLocation(DBMirror dbMirror, String tableName, Environment environment, String expectedLocation) {
        assertTrue("Table doesn't exist", dbMirror.getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", dbMirror.getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertEquals("Location doesn't match", expectedLocation, TableUtils.getLocation(tableName, dbMirror.getTableMirrors().get(tableName).getEnvironmentTable(environment).getDefinition()));
    }

    protected void validatePartitionLocation(DBMirror dbMirror, String tableName, Environment environment, String partitionSpec, String partitionLocation) {
        assertTrue("Table doesn't exist", dbMirror.getTableMirrors().containsKey(tableName));
        assertTrue("Environment doesn't exist", dbMirror.getTableMirrors().get(tableName).getEnvironments().containsKey(environment));
        assertTrue("Partition doesn't exist", dbMirror.getTableMirrors().get(tableName).getEnvironmentTable(environment).getPartitions().containsKey(partitionSpec));
        assertEquals("Location doesn't match", partitionLocation, dbMirror.getTableMirrors().get(tableName).getEnvironmentTable(environment).getPartitions().get(partitionSpec));
    }

    protected void validateDBLocation(DBMirror dbMirror, Environment environment, String expectedLocation) {
        assertTrue("DB Environment doesn't exist", dbMirror.getDBDefinitions().containsKey(environment));
        assertEquals("Location doesn't match", expectedLocation, dbMirror.getDBDefinitions().get(environment).get("LOCATION"));
    }

    protected void validateDBManagedLocation(DBMirror dbMirror, Environment environment, String expectedLocation) {
        assertTrue("DB Environment doesn't exist", dbMirror.getDBDefinitions().containsKey(environment));
        assertEquals("Managed Location doesn't match", expectedLocation, dbMirror.getDBDefinitions().get(environment).get("MANAGEDLOCATION"));
    }
}
