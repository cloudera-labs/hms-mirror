package com.streever.hadoop.hms.util;

import com.streever.hadoop.hms.mirror.Cluster;
import com.streever.hadoop.hms.mirror.MirrorConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

public class TableUtils {
    private static Logger LOG = LogManager.getLogger(TableUtils.class);

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
                        value = value.substring(0,value.length()-1);
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
                            value = value.substring(0,value.length()-1);
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
                            value = value.substring(0,value.length()-1);
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

    public static void updateTblProperty(String key, String value, List<String> tableDefinition) {
        // Search for property first.
        int tpIdx = tableDefinition.indexOf(TBL_PROPERTIES);

        for (int i = tpIdx + 1; i < tableDefinition.size() - 1; i++) {
            String line = tableDefinition.get(i).trim();
            String[] checkProperty = line.split("=");
            String checkKey = checkProperty[0].replace("'", "");
            if (checkKey.equals(key)) {
                // Found existing Property, replace it.
                tableDefinition.remove(i);
                StringBuilder sb = new StringBuilder();
                sb.append("'").append(key).append("'='");
                sb.append(value).append("'");
                if (line.trim().endsWith(",")) {
                    sb.append(",");
                }
                tableDefinition.add(i, sb.toString());
                break;
            }
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
