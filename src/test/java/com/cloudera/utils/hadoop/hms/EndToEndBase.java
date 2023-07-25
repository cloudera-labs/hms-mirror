package com.cloudera.utils.hadoop.hms;

import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EndToEndBase {
    protected static String homedir = System.getProperty("user.home");
    protected static String separator = System.getProperty("file.separator");

    private String outputDirBase = null;

    protected String getOutputDirBase() {
        if (outputDirBase == null) {
            outputDirBase = homedir + separator + "hms-mirror-reports" + separator +
                    DataState.getInstance().getUnique() + separator +
                    getClass().getSimpleName() + separator;
        }
        return outputDirBase;
    }

    protected DBMirror getResults(String resultsFileStr) {
        DBMirror rtn = null;
        try {
            System.out.println("Results file: " + resultsFileStr);
//            LOG.info("Check 'classpath' for test data file");
            URL resultURL = null;//this.getClass().getResource(getConfig().getLoadTestDataFile());
//            LOG.info("Checking filesystem for test data file");
            File resultsFile = new File(resultsFileStr);
            if (!resultsFile.exists())
                throw new RuntimeException("Couldn't locate results file: " + resultsFileStr);
            resultURL = resultsFile.toURI().toURL();
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

            String yamlCfgFile = IOUtils.toString(resultURL, StandardCharsets.UTF_8);
            rtn = mapper.readerFor(DBMirror.class).readValue(yamlCfgFile);
            // Set Config Databases;
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
        return rtn;
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
