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

import com.cloudera.utils.hadoop.hms.mirror.Cluster;
import com.cloudera.utils.hadoop.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hadoop.hms.mirror.MirrorConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class TableUtils {
    private static Logger LOG = LogManager.getLogger(TableUtils.class);

    public static final String CREATE = "CREATE";
    public static final String CREATE_TABLE = "CREATE TABLE";
    public static final String CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE";
    public static final String CREATE_VIEW = "CREATE VIEW";
    public static final String PARTITIONED_BY = "PARTITIONED BY";
    public static final String ROW_FORMAT_SERDE = "ROW FORMAT SERDE";
    public static final String STORED_AS_INPUTFORMAT = "STORED AS INPUTFORMAT";
    public static final String OUTPUTFORMAT = "OUTPUTFORMAT";
    public static final String LOCATION = "LOCATION";
    public static final String WITH_SERDEPROPERTIES = "WITH SERDEPROPERTIES (";
    public static final String PATH = "'path'=";
    public static final String CLUSTERED_BY = "CLUSTERED BY (";
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
    public static final String STAGE_TRANSFER_PARTITION_DESC = "Moving data to partitioned ({0}) transfer table";
    public static final String LOAD_DESC = "Loading table from Staging";
    public static final String LOAD_FROM_SHADOW_DESC = "Loading table from Shadow";
    public static final String LOAD_FROM_PARTITIONED_SHADOW_DESC = "Loading table from Partitioned ({0}) Shadow";
    public static final String EXPORT_TABLE = "EXPORT Table";
    public static final String IMPORT_TABLE = "IMPORT Table";
    public static final String ACID_NOT_ON = "This is an ACID table.  Turn on ACID migration `-ma|--migrate-acid`.";

    public static String getLocation(String tableName, List<String> tableDefinition) {
        LOG.trace("Getting table location data for: " + tableName);
        String location = null;
        int locIdx = tableDefinition.indexOf(LOCATION);
        if (locIdx > 0) {
            location = tableDefinition.get(locIdx + 1).trim().replace("'", "");
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
            for (String line: tableDefinition) {
                if (line.contains(MirrorConf.AVRO_SCHEMA_URL_KEY)) {
                    int lineIdx = tableDefinition.indexOf(line);
                    String[] parts = line.split("=");
                    LOG.debug("Old AVRO Schema location: " + parts[1]);
                    String replacedProperty = parts[0] + "='" + newLocation + "'";
                    // add back the comma if present before.
                    if (parts[1].trim().endsWith(",")) {
                        replacedProperty = replacedProperty + ",";
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
            for (int i = wspIdx+1;i<tableDefinition.size();i++) {
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
                    || line.trim().startsWith(OUTPUTFORMAT) || line.trim().startsWith(CLUSTERED_BY)) {
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
            int indOf = -1;
            for (String line : tblDef) {
                if (line != null && line.trim().startsWith(CLUSTERED_BY)) {
                    indOf = tblDef.indexOf(line);
                    break;
                }
            }
            if (indOf > -1) {
                // found
                // remove 3 lines at CLUSTERED BY
                    /* For example:
                    CLUSTERED BY (
                        vin)
                    INTO 10 BUCKETS
                     */
                for (int i = 0; i < 3; i++) {
                    tblDef.remove(indOf);
                }
                rtn = Boolean.TRUE;
            }
        }
        return rtn;
    }

    public static Boolean isManaged(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is 'managed'");
        if (tableDefinition == null) {
            throw new RuntimeException("Table definition for " + tableName + " is null.");
        }
        if (tableDefinition != null) {
            for (String line : tableDefinition) {
                if (line != null && line.startsWith(CREATE_TABLE)) {
                    rtn = Boolean.TRUE;
                    break;
                }
            }
        }
        return rtn;
    }

    public static Boolean isManaged(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        rtn = isManaged(envTable.getName(), envTable.getDefinition());
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

    public static Boolean makeExternal(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        if (isManaged(tableName, tableDefinition)) {
            LOG.debug("Converting table: " + tableName + " to EXTERNAL");
            for (String line : tableDefinition) {
                if (line.startsWith(CREATE_TABLE)) {
                    int indexCT = tableDefinition.indexOf(line);
                    String cet = line.replace(CREATE_TABLE, CREATE_EXTERNAL_TABLE);
                    tableDefinition.set(indexCT, cet);
                    rtn = Boolean.TRUE;
                }
            }
            // If ACID, remove transactional property to complete conversion to external.
            removeTblProperty(MirrorConf.TRANSACTIONAL, tableDefinition);
        }
        return rtn;
    }

    public static Boolean makeExternal(EnvironmentTable envTable) {
        return makeExternal(envTable.getName(), envTable.getDefinition());
    }

    /*
    Check that its a Hive table and not a connector like HBase, Kafka, RDBMS, etc.
     */
    public static Boolean isHiveNative(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is 'native' (not a connector [HBase, Kafka, etc])");
        if (tableDefinition == null) {
            throw new RuntimeException("Table definition for " + tableName + " is null.");
        }
        for (String line : tableDefinition) {
            if (line != null && line.trim().startsWith(LOCATION)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    /*
    Check that its a Hive table and not a connector like HBase, Kafka, RDBMS, etc.
     */
    public static Boolean isHiveNative(EnvironmentTable envTable) {
        Boolean rtn = Boolean.FALSE;
        rtn = isHiveNative(envTable.getName(), envTable.getDefinition());
        return rtn;
    }

    public static Boolean isExternal(EnvironmentTable envTable) {
        return isExternal(envTable.getName(), envTable.getDefinition());
    }

    public static Boolean isExternal(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is 'external'");
        for (String line : tableDefinition) {
            if (line.startsWith(CREATE_EXTERNAL_TABLE)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static Boolean isHive3Standard(String tableName, List<String> tableDefinition) {
        if (isManaged(tableName, tableDefinition) && !isACID(tableName, tableDefinition)) {
            return Boolean.FALSE;
        } else {
            return Boolean.TRUE;
        }
    }

    public static Boolean isHMSConverted(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' was converted by 'hms-mirror'");
        if (tableDefinition == null) {
            throw new RuntimeException("Table definition for " + tableName + " is null.");
        }
        for (String line : tableDefinition) {
            if (line != null) {
                String tline = line.trim();
                if (tline.toLowerCase().startsWith("'" + MirrorConf.HMS_MIRROR_CONVERTED_FLAG.toLowerCase())) {
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
        return isView(envTable.getName(), envTable.getDefinition());
    }

    public static Boolean isView(String name, List<String> definition) {
        Boolean rtn = Boolean.FALSE;
        if (definition == null) {
            throw new RuntimeException("Definition for " + name + " is null.");
        }

        for (String line : definition) {
            if (line.trim().startsWith(CREATE_VIEW)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static Boolean isACID(EnvironmentTable envTable) {
        return isACID(envTable.getName(), envTable.getDefinition());
    }

    public static Boolean isACID(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is 'transactional(ACID)'");
        if (tableDefinition == null) {
            throw new RuntimeException("Table definition for " + tableName + " is null.");
        }
        if (isManaged(tableName, tableDefinition)) {
            for (String line : tableDefinition) {
                if (line != null) {
                    String tline = line.trim();
                    if (tline.toLowerCase().startsWith("'" + MirrorConf.TRANSACTIONAL)) {
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
        }
        return rtn;
    }

    public static Boolean isExternalPurge(EnvironmentTable envTable) {
        return isExternalPurge(envTable.getName(), envTable.getDefinition());
    }

    public static Boolean isExternalPurge(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is an 'External' Purge table");
        if (tableDefinition == null) {
            throw new RuntimeException("Table definition for " + tableName + " is null.");
        }
        if (isExternal(tableName, tableDefinition)) {
            for (String line : tableDefinition) {
                if (line != null) {
                    String tline = line.trim();
                    if (tline.toLowerCase().startsWith("'" + MirrorConf.EXTERNAL_TABLE_PURGE)) {
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

    public static Boolean isPartitioned(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is 'Partitioned'");
        if (tableDefinition == null) {
            throw new RuntimeException("Table definition for " + tableName + " is null.");
        }
        for (String line : tableDefinition) {
            if (line != null && line.startsWith(PARTITIONED_BY)) {
                rtn = Boolean.TRUE;
                break;
            }
        }
        return rtn;
    }

    public static Boolean isAVROSchemaBased(EnvironmentTable envTable) {
        return isAVROSchemaBased(envTable.getName(), envTable.getDefinition());
    }

    public static Boolean isAVROSchemaBased(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is an AVRO table using a schema file in hcfs.");
        if (tableDefinition == null) {
            throw new RuntimeException("Table definition for " + tableName + " is null.");
        }
        for (String line : tableDefinition) {
            if (line != null && line.contains(MirrorConf.AVRO_SCHEMA_URL_KEY)) {
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
            if (line.contains(MirrorConf.AVRO_SCHEMA_URL_KEY)) {
                try {
                    String[] parts = line.split("=");
                    if (parts.length > 2) {
                        StringBuilder sb = new StringBuilder();
                        for (int i=1;i<parts.length;i++) {
                            sb.append(parts[i]);
                            if (i<parts.length-1)
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

    public static Boolean isLegacyManaged(Cluster cluster, String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        if (isManaged(tableName, tableDefinition) && cluster.getLegacyHive() && !isACID(tableName, tableDefinition)) {
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public static Boolean isHMSLegacyManaged(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' was tagged as Legacy Managed by 'hms-mirror'");
        for (String line : tableDefinition) {
            String tline = line.trim();
            if (tline.toLowerCase().startsWith("'" + MirrorConf.HMS_MIRROR_LEGACY_MANAGED_FLAG.toLowerCase())) {
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
                    if (line.trim().endsWith(",")) {
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
            if (checkKey.equals(key)) {
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
