package com.streever.hadoop.hms.util;

import com.streever.hadoop.hms.mirror.Cluster;
import com.streever.hadoop.hms.mirror.MirrorConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class TableUtils {
    private static Logger LOG = LogManager.getLogger(TableUtils.class);

    public static final String CREATE = "CREATE";
    public static final String CREATE_TABLE = "CREATE TABLE";
    public static final String CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE";
    public static final String PARTITIONED_BY = "PARTITIONED BY";
    public static final String TRANSACTIONAL = "transactional";
    public static final String EXTERNAL_PURGE = "external.table.purge";
    public static final String LOCATION = "LOCATION";
    public static final String TBL_PROPERTIES = "TBLPROPERTIES (";
//    public static final String HMS_CONVERTED = "CREATE TABLE";

    public static String getLocation(String tableName, List<String> tableDefinition) {
        LOG.trace("Getting table location data for: " + tableName);
        int locIdx = tableDefinition.indexOf(LOCATION);
        if (locIdx > 0) {
            String location = tableDefinition.get(locIdx + 1).trim().replace("'", "");
            return location;
        } else {
            return null;
        }
    }

    public static Boolean changeLocationNamespace(String tableName, List<String> tableDefinition,
                                                  String oldNamespace, String newNamespace) {
        LOG.trace("Changing table location namespace for: " + tableName);
        int locIdx = tableDefinition.indexOf(LOCATION);
        if (locIdx > 0) {
            String location = tableDefinition.get(locIdx + 1);
            String newLocation = location.replace(oldNamespace, newNamespace);
            tableDefinition.set(locIdx + 1, newLocation);
            return Boolean.TRUE;
        } else {
            return null;
        }

    }

    public static Boolean isManaged(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is 'managed'");
        for (String line : tableDefinition) {
            if (line.startsWith(CREATE_TABLE)) {
                rtn = Boolean.TRUE;
                break;
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
        if (isManaged(tableName, tableDefinition) && !isACID(tableName, tableDefinition)) {
            LOG.debug("Converting table: " + tableName + " to EXTERNAL");
            for (String line : tableDefinition) {
                if (line.startsWith(CREATE_TABLE)) {
                    int indexCT = tableDefinition.indexOf(line);
                    String cet = line.replace(CREATE_TABLE, CREATE_EXTERNAL_TABLE);
                    tableDefinition.set(indexCT, cet);
                    rtn = Boolean.TRUE;
                    break;
                }
            }
        }
        return rtn;
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
        for (String line : tableDefinition) {
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
        return rtn;
    }


    public static Boolean isACID(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is 'transactional(ACID)'");
        if (isManaged(tableName, tableDefinition)) {
            for (String line : tableDefinition) {
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
                        }
                    }
                    break;
                }
            }
        }
        return rtn;
    }

    public static Boolean isExternalPurge(String tableName, List<String> tableDefinition) {
        Boolean rtn = Boolean.FALSE;
        LOG.debug("Checking if table '" + tableName + "' is an 'External' Purge table");
        if (isExternal(tableName, tableDefinition)) {
            for (String line : tableDefinition) {
                String tline = line.trim();
                if (tline.toLowerCase().startsWith("'" + EXTERNAL_PURGE)) {
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
        for (String line : tableDefinition) {
            if (line.startsWith(PARTITIONED_BY)) {
                rtn = Boolean.TRUE;
                break;
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

    public static Boolean isHMSLegacyManaged(Cluster cluster, String tableName, List<String> tableDefinition) {
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
