/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.util;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.mirror.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.DATA_SIZE;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.FILE_FORMAT;
import static com.cloudera.utils.hadoop.hms.mirror.TablePropertyVars.*;

public class TableUtils {
    private static final Logger LOG = LogManager.getLogger(TableUtils.class);

    public static final String CREATE = "CREATE";
    public static final String CREATE_TABLE = "CREATE TABLE";
    public static final String CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE";
    public static final String CREATE_VIEW = "CREATE VIEW";
    public static final String PARTITIONED_BY = "PARTITIONED BY";
    public static final String ROW_FORMAT_DELIMITED = "ROW FORMAT DELIMITED";
    public static final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    public static final String STORED_AS_INPUTFORMAT = "STORED AS INPUTFORMAT";
    public static final String OUTPUTFORMAT = "OUTPUTFORMAT";
    public static final String LOCATION = "LOCATION";
    public static final String WITH_SERDEPROPERTIES = "WITH SERDEPROPERTIES (";
    public static final String PATH = "'path'=";
    public static final String CLUSTERED_BY = "CLUSTERED BY (";
    public static final String BUCKETS = "BUCKETS";
    public static final String INTO = "INTO";
    public static final String TBL_PROPERTIES = "TBLPROPERTIES (";
    public static final String USE_DESC = "Selecting DB";
    public static final String CREATE_DESC = "Creating Table";
    public static final String CREATE_SHADOW_DESC = "Creating Shadow Table";
    public static final String CREATE_TRANSFER_DESC = "Creating Transfer Table";
    public static final String DROP_DESC = "Dropping Table";
    public static final String DROP_SHADOW_TABLE = "Dropping Shadow Table";
    public static final String DROP_TRANSFER_TABLE = "Dropping Transfer Table";
    public static final String REPAIR_DESC = "Repairing Table (MSCK)";
    public static final String STAGE_TRANSFER_DESC = "Moving data to transfer table";
    public static final String STORAGE_MIGRATION_TRANSFER_DESC = "Moving data to new Namespace";
    public static final String STAGE_TRANSFER_PARTITION_DESC = "Moving data to partitioned ({0}) transfer table";
    public static final String STORAGE_MIGRATION_TRANSFER_PARTITION_DESC = "Moving partitioned ({0}) data to new Namespace";
    public static final String LOAD_DESC = "Loading table from Staging";
    public static final String LOAD_FROM_SHADOW_DESC = "Loading table from Shadow";
    public static final String LOAD_FROM_PARTITIONED_SHADOW_DESC = "Loading table from Partitioned ({0}) Shadow";
    public static final String EXPORT_TABLE = "EXPORT Table";
    public static final String IMPORT_TABLE = "IMPORT Table";
    public static final String RENAME_TABLE = "RENAME Table";
    public static final String ACID_NOT_ON = "This is an ACID table.  Turn on ACID migration `-ma|--migrate-acid`.";
    public static Pattern tableCreatePattern = Pattern.compile(".*TABLE `?([a-z,A-Z,_,0-9,_]+)`?\\.?`?([a-z,A-Z,_,0-9,_]+)?");
//    public static Pattern dbdottable = Pattern.compile(".*`?\\.`?(.*)");

    public static String getLocation(String tableName, List<String> tableDefinition) {
        LOG.trace("Getting table location data for: " + tableName);
        String location = null;
        int locIdx = tableDefinition.indexOf(LOCATION);
        if (locIdx > 0) {
            location = tableDefinition.get(locIdx + 1).trim().replace("'", "");
        }
        return location;
    }

    public static String getTableNameFromDefinition(List<String> tableDefinition) {
        String tableName = null;
        for (String line : tableDefinition) {
            LOG.debug("Tablename Check: " + line);
            if (line.contains("CREATE")) {
                Matcher matcher = tableCreatePattern.matcher(line);
                if (matcher.find()) {
                    if (matcher.groupCount() == 3) {
                        if (matcher.group(3) == null)
                            tableName = matcher.group(2);
                        else
                            tableName = matcher.group(1);
                    } else if (matcher.groupCount() == 2) {
                        if (matcher.group(2) == null)
                            tableName = matcher.group(1);
                        else
                            tableName = matcher.group(2);
                    }
                    break;
                } else {
                    LOG.error("Couldn't locate tablename in: " + line);
                }
            }
        }
        return tableName;
    }

    public static Boolean doesTableNameMatchDirectoryName(List<String> tableDefinition) {
        String tableName = getTableNameFromDefinition(tableDefinition);
        return doesTableNameMatchDirectoryName(tableName, tableDefinition);
    }

    public static Boolean doesTableNameMatchDirectoryName(String tableName, List<String> tableDefinition) {
        String location = getLocation(tableName, tableDefinition);
        int idx = location.lastIndexOf('/');
        String dirName = location.substring(idx + 1);
        if (tableName.equals(dirName)) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public static String getSerdePath(String tableName, List<String> tableDefinition) {
        LOG.trace("Getting table serde path (if available) data for: " + tableName);
        String location = null;

        int wspIdx = tableDefinition.indexOf(WITH_SERDEPROPERTIES);
        if (wspIdx > 0) {
            for (int i = wspIdx + 1; i < tableDefinition.size(); i++) {
                String sprop = tableDefinition.get(i);
                if (sprop.trim().startsWith(PATH)) {
                    String[] pathLine = sprop.split("=");
                    if (pathLine.length == 2) {
                        if (pathLine[1].startsWith("'")) {
                            location = pathLine[1].substring(1, pathLine[1].length() - 2);
                        }
                    }
                    break;
                }
            }
        }

        return location;
    }


    public static String prefixTableNameLocation(EnvironmentTable envTable, String prefix) {
        return prefixTableNameLocation(envTable.getName(), envTable.getDefinition(), prefix);
    }

    public static String prefixTableNameLocation(String tableName, List<String> tableDefinition, String prefix) {
        LOG.trace("Prefix table location data for: " + tableName);
        String location = null;
        int locIdx = tableDefinition.indexOf(LOCATION);
        if (locIdx > 0) {
            location = tableDefinition.get(locIdx + 1).trim();
            int lastSlashIdx = location.lastIndexOf("/");
            location = location.substring(0, lastSlashIdx) + "/" + prefix + location.substring(lastSlashIdx + 1);
        }
        return location;
    }

    public static List<String> stripLocation(EnvironmentTable envTable) {
        return stripLocation(envTable.getName(), envTable.getDefinition());
    }

    public static List<String> stripLocation(String tableName, List<String> tableDefinition) {
        LOG.trace("Stripping table location data for: " + tableName);
        String location = null;
        int locIdx = tableDefinition.indexOf(LOCATION);
        if (locIdx > 0) {
            tableDefinition.remove(locIdx + 1);
            tableDefinition.remove(locIdx);
        }
        return tableDefinition;
    }

    public static List<String> changeTableName(EnvironmentTable envTable, String newTableName) {
        List<String> rtn = changeTableName(envTable.getName(), newTableName, envTable.getDefinition());
        envTable.setName(newTableName);
        return rtn;
    }

    public static List<String> changeTableName(String tableName, String newTableName, List<String> tableDefinition) {
        LOG.trace("Changing name of table in definition");
        for (String line : tableDefinition) {
            if (line.startsWith(CREATE)) {
                int indexCT = tableDefinition.indexOf(line);
                // Split on the period between the db and table
                String createLine = tableDefinition.get(indexCT);
                createLine = createLine.replace(tableName, newTableName);
                tableDefinition.set(indexCT, createLine);
                break;
            }
        }
        return tableDefinition;
    }

    public static Boolean updateAVROSchemaLocation(EnvironmentTable envTable, String newLocation) {
        return updateAVROSchemaLocation(envTable.getName(), envTable.getDefinition(), newLocation);
    }

    public static Boolean updateAVROSchemaLocation(String tableName, List<String> tableDefinition, String newLocation) {
        Boolean rtn = Boolean.FALSE;
        LOG.trace("Updating AVRO Schema URL: " + tableName);

        if (newLocation != null) {
            for (String line : tableDefinition) {
                if (line.contains(AVRO_SCHEMA_URL_KEY)) {
                    int lineIdx = tableDefinition.indexOf(line);
                    String[] parts = line.split("=");
                    LOG.debug("Old AVRO Schema location: " + parts[1]);
                    String replacedProperty = parts[0] + "='" + newLocation + "'";
                    // add back the comma if present before.
                    if (parts[1].trim().endsWith(",")) {
                        replacedProperty = replacedProperty + ",";
                    }
                    if (parts[1].trim().endsWith(")")) {
                        replacedProperty = replacedProperty + ")";
                    }
                    tableDefinition.set(lineIdx, replacedProperty);
                    LOG.debug("Replaced AVRO Schema URL Property: " + replacedProperty);
                    rtn = Boolean.TRUE;
                    break;
                }
            }
        }
        return rtn;
    }

    public static Boolean updateTableLocation(EnvironmentTable envTable, String newLocation) {
        return updateTableLocation(envTable.getName(), envTable.getDefinition(), newLocation);
    }

    public static Boolean updateTableLocation(String tableName, List<String> tableDefinition,
                                              String newLocation) {
        Boolean rtn = Boolean.FALSE;
        LOG.trace("Updating table location for: " + tableName);

        if (newLocation != null) {
            int locIdx = tableDefinition.indexOf(LOCATION);
            if (locIdx >= 0) {
                // Removing preexisting quotes before adding them back.
                tableDefinition.set(locIdx + 1, "'" + newLocation.replaceAll("'", "") + "'");
                rtn = Boolean.TRUE;
            }
        }
        // Check for a 'path' element in SERDEPROPERTIES.  This is set by spark in some case and it matches the LOCATION
        // path.
        int wspIdx = tableDefinition.indexOf(WITH_SERDEPROPERTIES);
        if (wspIdx > 0) {
            for (int i = wspIdx + 1; i < tableDefinition.size(); i++) {
                String sprop = tableDefinition.get(i);
                if (sprop.trim().startsWith(PATH)) {
                    String rprop = "'path'='" + newLocation.replaceAll("'", "") + "'";
                    if (sprop.trim().endsWith(","))
                        rprop = rprop + ",";
                    if (sprop.trim().endsWith(")"))
                        rprop = rprop + ")";
                    tableDefinition.set(i, rprop);
                    break;
                }
            }
        }
        return rtn;
    }

    public static int numOfBuckets(EnvironmentTable envTable) {
        int rtn = 0;
        LOG.trace("Looking to see if table has buckets");
        for (String line : envTable.getDefinition()) {
            if (line.startsWith(INTO)) {
                String[] bucketParts = line.split(" ");
                rtn = Integer.valueOf(bucketParts[1]);
                break;
            }
        }
        return rtn;
    }

    public static String toPartitionSpec(String simplePartName) {
        String parts[] = simplePartName.split("/");
        // Look at each parts and split on the equals sign, then concat the parts back together putting a single quote
        // around the value and a comma between each part. Do not leave a trailing comma.
        String partSpec = Arrays.stream(parts).map(
                part -> part.split("=")
        ).map(
                partElements -> partElements[0] + "=\"" + partElements[1] + "\""
        ).collect(
                Collectors.joining(",")
        );
        return partSpec;
    }

    public static String getPartitionElements(EnvironmentTable envTable) {
        String rtn = null;
        // LOCATE the "PARTITIONED BY" line.
        int pIdx = 0;
        int pEIdx = 0;
        for (String line : envTable.getDefinition()) {
            if (line.trim().startsWith(PARTITIONED_BY)) {
                // Get index of line.
                pIdx = envTable.getDefinition().indexOf(line);
            }
            if (line.trim().startsWith(ROW_FORMAT_SERDE) || line.trim().startsWith(STORED_AS_INPUTFORMAT)
                    || line.trim().startsWith(OUTPUTFORMAT) || line.trim().startsWith(CLUSTERED_BY)
                    || line.trim().startsWith(ROW_FORMAT_DELIMITED)) {
                pEIdx = envTable.getDefinition().indexOf(line);
            }
            if (pIdx > 0 && pEIdx > 0) {
                // we have our markers. break from loop.
                break;
            }
        }
        if (pIdx < pEIdx) {
            StringBuilder sb = new StringBuilder();
            for (int i = pIdx + 1; i < pEIdx; i++) {
                String[] parts = envTable.getDefinition().get(i).trim().split(" ");
                // NOTE: The element definition should already be quoted.
                sb.append(parts[0]);
                if (i < pEIdx - 1)
                    sb.append(",");
            }
            rtn = sb.toString();
        }
        return rtn;
    }

    /*
    Remove bucket definition if the bucket count is BELOW the artificialBucketThreshold.

    The concept here is to eliminate bucket definitions that were artificial.  If the bucket
    count is ABOVE the threshold we leave it intact, as it was probably an intentional design
    aspect of the table.

     */
    public static Boolean removeBuckets(EnvironmentTable envTable, int threshold) {
        Boolean rtn = Boolean.FALSE;

        // Bucket Reduction count that's less than 0 means don't alter.
        if (threshold >= 0 && numOfBuckets(envTable) <= threshold) {
            // If the bucketCount is <= 0, remove bucket definition.
            List<String> tblDef = envTable.getDefinition();
            int clusterIndOf = -1;
            int bucketIndOf = -1;
            for (String line : tblDef) {
                if (line != null && line.trim().startsWith(CLUSTERED_BY)) {
                    clusterIndOf = tblDef.indexOf(line);
                }
                if (clusterIndOf > -1) {
                    // Look for bucket index
                    if (line != null && line.trim().contains(BUCKETS)) {
                        bucketIndOf = tblDef.indexOf(line);
                        break;
                    }
                }
            }
            if (clusterIndOf > -1 && bucketIndOf > -1) {
                // found
                // remove 3 lines at CLUSTERED BY
                    /* For example:
                    CLUSTERED BY (
                        vin)
                    INTO 10 BUCKETS
                     */
                int diff = bucketIndOf - clusterIndOf;
                for (int i = 0; i <= diff; i++) {
                    tblDef.remove(clusterIndOf);
                }
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public static SerdeType getSerdeType(EnvironmentTable envTable) {
        LOG.trace("Getting table location data for: " + envTable.getName());
        String serdeClass = null;
        SerdeType rtn = SerdeType.UNKNOWN;
        int locIdx = envTable.getDefinition().indexOf(ROW_FORMAT_SERDE);
        if (locIdx > 0) {
            serdeClass = envTable.getDefinition().get(locIdx + 1).trim().replace("'", "");
        }
        if (serdeClass != null) {
            for (SerdeType serdeType : SerdeType.values()) {
                if (serdeType.isType(serdeClass)) {
                    rtn = serdeType;
                    break;
                }
            }
        }
        envTable.getStatistics().put(FILE_FORMAT, rtn);
        return rtn;
    }

    public static Boolean isManaged(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' is 'managed'");
        if (envTable.getDefinition() == null) {
            throw new RuntimeException("Table definition for " + envTable.getName() + " is null.");
        }
        if (envTable.getDefinition() != null) {
            for (String line : envTable.getDefinition()) {
                if (line != null && line.startsWith(CREATE_TABLE)) {
                    rtn = Boolean.TRUE;
                    break;
                }
            }
        }
        return rtn;
    }

    public static void stripDatabase(String tableName, List<String> tableDefinition) {
        for (String line : tableDefinition) {
            if (line.startsWith(CREATE)) {
                int indexCT = tableDefinition.indexOf(line);
                // Split on the period between the db and table
                String[] parts = line.split("\\.");
                if (parts.length == 2) {
                    // Now split on the `
                    String[] parts01 = parts[0].split("`");
                    if (parts01.length == 2) {
                        String newCreate = parts01[0] + "`" + parts[1];
                        tableDefinition.set(indexCT, newCreate);
                    }
                }
                break;
            }
        }
    }

    public static void stripDatabase(EnvironmentTable envTable) {
        for (String line : envTable.getDefinition()) {
            if (line.startsWith(CREATE)) {
                int indexCT = envTable.getDefinition().indexOf(line);
                // Split on the period between the db and table
                String[] parts = line.split("\\.");
                if (parts.length == 2) {
                    // Now split on the `
                    String[] parts01 = parts[0].split("`");
                    if (parts01.length == 2) {
                        String newCreate = parts01[0] + "`" + parts[1];
                        envTable.getDefinition().set(indexCT, newCreate);
                    }
                }
                break;
            }
        }
    }


    public static Boolean prefixTableName(String tableName, String prefix, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Prefixing table: " + tableName + " with " + prefix);
        for (String line : tableDefinition) {
            if (line.startsWith(CREATE)) {
                int indexCT = tableDefinition.indexOf(line);
                String[] parts = line.split("`");
                if (parts.length == 3) {
                    parts[1] = prefix + parts[1];
                    String newCreateline = String.join("`", parts);
                    tableDefinition.set(indexCT, newCreateline);
                    rtn = Boolean.TRUE;
                    break;
                } else {
                    // problem
                }
            }
        }
        return rtn;
    }

    public static Boolean makeExternal(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        if (isManaged(envTable)) {
            LOG.debug("Converting table: " + envTable.getName() + " to EXTERNAL");
            for (String line : envTable.getDefinition()) {
                if (line.startsWith(CREATE_TABLE)) {
                    int indexCT = envTable.getDefinition().indexOf(line);
                    String cet = line.replace(CREATE_TABLE, CREATE_EXTERNAL_TABLE);
                    envTable.getDefinition().set(indexCT, cet);
                    rtn = Boolean.TRUE;
                }
            }
            // If ACID, remove transactional property to complete conversion to external.
            removeTblProperty(TRANSACTIONAL, envTable.getDefinition());
            removeTblProperty(TRANSACTIONAL_PROPERTIES, envTable.getDefinition());
            removeTblProperty(BUCKETING_VERSION, envTable.getDefinition());
        }
        return rtn;
    }

    public static Boolean fixTableDefinition(EnvironmentTable environmentTable) {
        return fixTableDefinition(environmentTable.getDefinition());
    }

    /*
    Fix any potention syntax issues.
     */
    public static Boolean fixTableDefinition(List<String> tableDefinition) {
        // Remove trailing ',' from TBL_PROPERTIES.
        int tpIdx = tableDefinition.indexOf(TBL_PROPERTIES);
        if (tpIdx != -1) {
            boolean hangingParen = tableDefinition.get(tableDefinition.size() - 1).trim().equals(")") ? Boolean.TRUE : Boolean.FALSE;
            int checkLineNum = tableDefinition.size() - 1;
            if (hangingParen) {
                checkLineNum = tableDefinition.size() - 2;
            }
            for (int i = tpIdx + 1; i < tableDefinition.size() - 1; i++) {
                String line = tableDefinition.get(i).trim();

                if (i >= checkLineNum) {
                    if (line.endsWith(",")) {
                        // need to remove comma.
                        String newLine = line.substring(0, line.length() - 1);
//                        tableDefinition.remove(i);
                        // Replace without comma
                        tableDefinition.set(i, newLine);
                    }
                }
            }
        }

        return Boolean.TRUE;
    }

    /*
    Check that its a Hive table and not a connector like HBase, Kafka, RDBMS, etc.
     */
    public static Boolean isHiveNative(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' is 'native' (not a connector [HBase, Kafka, etc])");
        if (envTable.getDefinition() == null) {
            throw new RuntimeException("Table definition for " + envTable.getName() + " is null.");
        }
        for (String line : envTable.getDefinition()) {
            if (line != null && line.trim().startsWith(LOCATION)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static Boolean isExternal(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' is 'external'");
        for (String line : envTable.getDefinition()) {
            if (line.startsWith(CREATE_EXTERNAL_TABLE)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static Boolean isHive3Standard(EnvironmentTable envTable) {
        if (isManaged(envTable) && !isACID(envTable)) {
            return Boolean.FALSE;
        } else {
            return Boolean.TRUE;
        }
    }

    public static Boolean isHMSConverted(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' was converted by 'hms-mirror'");
        if (envTable.getDefinition() == null) {
            throw new RuntimeException("Table definition for " + envTable.getName() + " is null.");
        }
        for (String line : envTable.getDefinition()) {
            if (line != null) {
                String tline = line.trim();
                if (tline.toLowerCase().startsWith("'" + HMS_MIRROR_CONVERTED_FLAG.toLowerCase())) {
                    String[] prop = tline.split("=");
                    if (prop.length == 2) {
                        // Stripe the quotes
                        String value = prop[1].replace("'", "").trim();
                        // Remove trailing , or )
                        if (value.endsWith(",") || value.endsWith(")")) {
                            value = value.substring(0, value.length() - 1);
                        }
                        if (Boolean.valueOf(value)) {
                            rtn = Boolean.TRUE;
                        }
                    }
                    break;
                }
            }
        }
        return rtn;
    }

    public static Boolean isView(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        if (envTable.getDefinition() == null) {
            throw new RuntimeException("Definition for " + envTable.getName() + " is null.");
        }

        for (String line : envTable.getDefinition()) {
            if (line.trim().startsWith(CREATE_VIEW)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static Boolean isACID(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' is 'transactional(ACID)'");
        if (envTable.getDefinition() == null) {
            throw new RuntimeException("Table definition for " + envTable.getName() + " is null.");
        }
        if (isManaged(envTable)) {
            for (String line : envTable.getDefinition()) {
                if (line != null) {
                    String tline = line.trim();
                    if (tline.toLowerCase().startsWith("'" + TRANSACTIONAL)) {
                        String[] prop = tline.split("=");
                        if (prop.length == 2) {
                            // Stripe the quotes
                            String value = prop[1].replace("'", "").trim();
                            // Remove trailing , or )
                            if (value.endsWith(",") || value.endsWith(")")) {
                                value = value.substring(0, value.length() - 1);
                            }
                            if (Boolean.valueOf(value)) {
                                rtn = Boolean.TRUE;
                                envTable.getStatistics().put(TRANSACTIONAL, Boolean.TRUE);
                            }
                        }
                        break;
                    }
                }
            }
        }
        return rtn;
    }

    public static Boolean isExternalPurge(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' is an 'External' Purge table");
        if (envTable.getDefinition() == null) {
            throw new RuntimeException("Table definition for " + envTable.getName() + " is null.");
        }
        if (isExternal(envTable)) {
            for (String line : envTable.getDefinition()) {
                if (line != null) {
                    String tline = line.trim();
                    if (tline.toLowerCase().startsWith("'" + EXTERNAL_TABLE_PURGE)) {
                        String[] prop = tline.split("=");
                        if (prop.length == 2) {
                            // Stripe the quotes
                            String value = prop[1].replace("'", "").trim();
                            // Remove trailing , or )
                            if (value.endsWith(",") || value.endsWith(")")) {
                                value = value.substring(0, value.length() - 1);
                            }
                            if (Boolean.valueOf(value)) {
                                rtn = Boolean.TRUE;
                                envTable.getStatistics().put(EXTERNAL_TABLE_PURGE, Boolean.TRUE);
                            }
                        }
                        break;
                    }
                }
            }
        }
        return rtn;
    }

    public static String tableFieldsFingerPrint(List<String> tableDef) {
        String hashText = null;
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < tableDef.size(); i++) {
            String item2 = tableDef.get(i);
            if (!item2.equals(LOCATION)) {
                sb.append(item2.trim());
            } else {
                break;
            }
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");

            byte[] messageDigest = md.digest(sb.toString().getBytes());

            BigInteger no = new BigInteger(1, messageDigest);
            hashText = no.toString(16);
            while (hashText.length() < 32) {
                hashText = "0" + hashText;
            }
        } catch (NoSuchAlgorithmException nsae) {
            throw new RuntimeException(nsae);
        }
        return hashText;
    }


    public static Boolean isPartitioned(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' is 'Partitioned'");
        if (envTable.getDefinition() == null) {
            return rtn;
        }
        for (String line : envTable.getDefinition()) {
            if (line != null && line.startsWith(PARTITIONED_BY)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static Boolean isAVROSchemaBased(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' is an AVRO table using a schema file in hcfs.");
        if (envTable.getDefinition() == null) {
            throw new RuntimeException("Table definition for " + envTable.getName() + " is null.");
        }
        for (String line : envTable.getDefinition()) {
            if (line != null && line.contains(AVRO_SCHEMA_URL_KEY)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static String getAVROSchemaPath(EnvironmentTable envTable) {
        return getAVROSchemaPath(envTable.getName(), envTable.getDefinition());
    }

    public static String getAVROSchemaPath(String tblName, List<String> tblDefinition) {
        String rtn = null;
        LOG.debug("Retrieving AVRO Schema Path for " + tblName);
        for (String line : tblDefinition) {
            if (line.contains(AVRO_SCHEMA_URL_KEY)) {
                try {
                    String[] parts = line.split("=");
                    if (parts.length > 2) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 1; i < parts.length; i++) {
                            sb.append(parts[i]);
                            if (i < parts.length - 1)
                                sb.append("=");
                        }
                        rtn = sb.toString().replace("'", " ").replace(",", "").trim();
                    } else {
                        // Stripe Quotes
                        rtn = parts[1].replace("'", " ").replace(",", "").trim();
                    }
                    break;
                } catch (Throwable t) {
                    // Nothing, just return null.
                }
            }
        }
        return rtn;
    }

    public static Boolean isLegacyManaged(Cluster cluster, EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        if (isManaged(envTable) && cluster.getLegacyHive() && !isACID(envTable)) {
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public static Boolean isHMSLegacyManaged(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + envTable.getName() + "' was tagged as Legacy Managed by 'hms-mirror'");
        for (String line : envTable.getDefinition()) {
            String tline = line.trim();
            if (tline.toLowerCase().startsWith("'" + HMS_MIRROR_LEGACY_MANAGED_FLAG.toLowerCase())) {
                String[] prop = tline.split("=");
                if (prop.length == 2) {
                    // Stripe the quotes
                    String value = prop[1].replace("'", "").trim();
                    // Remove trailing , or )
                    if (value.endsWith(",") || value.endsWith(")")) {
                        value = value.substring(0, value.length() - 1);
                    }
                    if (Boolean.valueOf(value)) {
                        rtn = Boolean.TRUE;
                    }
                }
                break;
            }
        }
        return rtn;
    }

    public static void upsertTblProperty(String key, String value, EnvironmentTable envTable) {
        upsertTblProperty(key, value, envTable.getDefinition());
    }

    public static void upsertTblProperty(String key, String value, List<String> tableDefinition) {
        // Search for property first.
        int tpIdx = tableDefinition.indexOf(TBL_PROPERTIES);
        if (tpIdx != -1) {
            boolean found = false;
            for (int i = tpIdx + 1; i < tableDefinition.size() - 1; i++) {
                String line = tableDefinition.get(i).trim();
                String[] checkProperty = line.split("=");
                String checkKey = checkProperty[0].replace("'", "");
//                checkKey = checkKey.replace(",", "");
                if (checkKey.equals(key)) {
                    // Found existing Property, replace it.
                    tableDefinition.remove(i);
                    StringBuilder sb = new StringBuilder();
                    if (value != null) {
                        sb.append("'").append(key).append("'='");
                        sb.append(value).append("'");
                    } else {
                        sb.append("'").append(key).append("'");
                    }
                    if (i < tableDefinition.size()) {
//                    if (line.trim().endsWith(",")) {
                        sb.append(",");
                    }
                    tableDefinition.add(i, sb.toString());
                    found = true;
                    break;
                }
            }
            if (!found) {
                // Add Property.
                String newProp = null;
                if (value != null) {
                    newProp = "'" + key + "'='" + value + "',";
                } else {
                    newProp = "'" + key + "',";
                }
                tableDefinition.add(tpIdx + 1, newProp);
            }
        } else {
            // TODO: Need to be more aggressive here with this error
            LOG.error("Issue locating TBLPROPERTIES for table");
        }
    }

    public static StorageType getStorageType(List<String> tblDef) {
//        String rtn = null;
        int tpIdx = tblDef.indexOf("ROW FORMAT SERDE");
        String rowformatSerde = tblDef.get(tpIdx + 1);
        tpIdx = tblDef.indexOf("STORED AS INPUTFORMAT");
        String inputFormat = tblDef.get(tpIdx + 1);
        StorageType storageType = StorageType.from(rowformatSerde, inputFormat);

        return storageType;
    }

    public static boolean hasTblProperty(String key, EnvironmentTable environmentTable) {
        if (getTblProperty(key, environmentTable) != null) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public static boolean hasTblProperty(String key, List<String> tblDef) {
        if (getTblProperty(key, tblDef) != null) {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    /*
      Returns: IcebergState
     */
    public IcebergState getIcebergConversionState(EnvironmentTable envTable) {

        IcebergState rtn = IcebergState.NOT_CONVERTABLE;

        // TODO: WIP for Iceberg Hive table Conversion.

        return rtn;
    }

    public static String getTblProperty(String key, EnvironmentTable environmentTable) {
        return getTblProperty(key, environmentTable.getDefinition());
    }

    public static String getTblProperty(String key, List<String> tblDef) {
        String rtn = null;
        int tpIdx = tblDef.indexOf(TBL_PROPERTIES);

        for (int i = tpIdx + 1; i < tblDef.size(); i++) {
            String line = tblDef.get(i).trim();
            String[] checkProperty = line.split("=");
            String checkKey = checkProperty[0].replace("'", "");
            if (checkKey.equalsIgnoreCase(key)) {
                // Found existing Property, replace it.
                rtn = checkProperty[1].replace("'", "");
                break;
            }
        }
        // Remove Comma, if present.
        if (rtn != null && rtn.endsWith(","))
            rtn = rtn.substring(0, rtn.length() - 1);
        return rtn;
    }

    public static Boolean replaceTblProperty(String key, String newValue, EnvironmentTable environmentTable) {
        Boolean rtn = Boolean.FALSE;
        List<String> tblDef = environmentTable.getDefinition();
        int tpIdx = tblDef.indexOf(TBL_PROPERTIES);

        for (int i = tpIdx + 1; i < tblDef.size(); i++) {
            String line = tblDef.get(i).trim();
            String[] checkProperty = line.split("=");
            String checkKey = checkProperty[0].replace("'", "");
            if (checkKey.equalsIgnoreCase(key)) {
                // Found existing Property, replace it.
                tblDef.remove(i);
                StringBuilder sb = new StringBuilder();
                sb.append("'").append(key).append("'")
                        .append("=")
                        .append("'").append(newValue).append("'");
                // Replace ending param.
                if (line.endsWith(")")) {
                    sb.append(")");
                }
                tblDef.add(i, sb.toString());
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }


    public static void removeTblProperty(String key, EnvironmentTable envTable) {
        // Search for property first.
        removeTblProperty(key, envTable.getDefinition());
    }

    public static void removeTblProperty(String key, List<String> tableDefinition) {
        // Search for property first.
        int tpIdx = tableDefinition.indexOf(TBL_PROPERTIES);

        for (int i = tpIdx + 1; i < tableDefinition.size(); i++) {
            String line = tableDefinition.get(i).trim();
            String[] checkProperty = line.split("=");
            String checkKey = checkProperty[0].replace("'", "");
            if (checkKey.equalsIgnoreCase(key)) {
                // Found existing Property, replace it.
                tableDefinition.remove(i);
                // Replace ending param.
                if (line.endsWith(")")) {
                    if (i == tpIdx + 2) {
                        String lastLine = tableDefinition.get(i - 1).trim();
                        String newLastLine = lastLine.replace(",", ")");
                        tableDefinition.remove(i - 1);
                        tableDefinition.add(newLastLine);
                    } else {
                        tableDefinition.add(i, ")");
                    }
                }
                break;
            }
        }
    }

}
