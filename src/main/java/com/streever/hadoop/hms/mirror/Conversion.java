package com.streever.hadoop.hms.mirror;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class Conversion {
    @JsonIgnore
    private Date start = new Date();
    private Config config;
    private Map<String, DBMirror> databases = new TreeMap<String, DBMirror>();

    public Conversion() {
    }

    public Conversion(Config config) {
        this.config = config;
    }

    public DBMirror addDatabase(String database) {
        if (databases.containsKey(database)) {
            return databases.get(database);
        } else {
            DBMirror dbs = new DBMirror(database);
            databases.put(database, dbs);
            return dbs;
        }
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Map<String, DBMirror> getDatabases() {
        return databases;
    }

    public void setDatabases(Map<String, DBMirror> databases) {
        this.databases = databases;
    }

    public DBMirror getDatabase(String database) {
        return databases.get(database);
    }

    public String executeSql(String database) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- EXECUTION script for ").append(Environment.RIGHT).append(" cluster\n\n");
        sb.append("-- ").append(new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
        sb.append("-- These are the command run on the RIGHT cluster when `-e` is used.\n");
        DBMirror dbMirror = databases.get(database);
        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            sb.append("--    Table: ").append(table).append("\n");
            if (tblMirror.isThereSql()) {
                for (String sql : tblMirror.getSql()) {
                    sb.append(sql).append(";\n");
                }
            } else {
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public String actionsSql(Environment env, String database) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- ACTION script for ").append(env).append(" cluster\n\n");
        sb.append("-- HELPER Script to assist with MANUAL updates.\n");
        sb.append("-- RUN AT OWN RISK  !!!\n");
        sb.append("-- REVIEW and UNDERSTAND the adjustments below before running.\n\n");
        DBMirror dbMirror = databases.get(database);
        sb.append("-- DATABASE: ").append(database).append("\n");
        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            sb.append("--    Table: ").append(table).append("\n");
            // LEFT Table Actions
            Iterator<String> a1Iter = tblMirror.getTableActions(env).iterator();
            while (a1Iter.hasNext()) {
                String item = a1Iter.next();
                sb.append(item).append(";\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    public String toReport(Config config, String database) throws JsonProcessingException {
        StringBuilder sb = new StringBuilder();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sb.append("# HMS-Mirror for: ").append(database).append("\n\n");
        sb.append(ReportingConf.substituteVariablesFromManifest("v.${Implementation-Version}")).append("\n");
        sb.append("---\n").append("## Run Log\n\n");
        sb.append("| Date | Elapsed Time |\n");
        sb.append("|:---|:---|\n");
        Date current = new Date();
        BigDecimal elsecs = new BigDecimal(current.getTime() - start.getTime()).divide(new BigDecimal(1000));
        DecimalFormat eldecf = new DecimalFormat("#,###.00");
        String elsecStr = eldecf.format(elsecs);

        sb.append("| ").append(df.format(new Date())).append(" | ").append(elsecStr).append(" secs |\n\n");

        sb.append("## Config:\n");

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        String yamlStr = mapper.writeValueAsString(config);
        // Mask User/Passwords in Control File
        yamlStr = yamlStr.replaceAll("user:\\s\".*\"", "user: \"*****\"");
        yamlStr = yamlStr.replaceAll("password:\\s\".*\"", "password: \"*****\"");
        sb.append("```\n");
        sb.append(yamlStr).append("\n");
        sb.append("```\n\n");

        DBMirror dbMirror = databases.get(database);
        sb.append("## DB Create Statement").append("\n\n");
        sb.append("```").append("\n");
        sb.append(dbMirror.rightDBCreate(config)[0]);
        sb.append("```").append("\n");

        sb.append("\n");

        sb.append("## DB Issues").append("\n\n");
        // Issues
        if (dbMirror.isThereAnIssue()) {
            for (String issue : dbMirror.getIssues()) {
                sb.append("* ").append(issue).append("\n");
            }
        } else {
            sb.append("none\n");
        }

        sb.append("\n## Table Status").append("\n\n");

        sb.append("<table>").append("\n");
        sb.append("<tr>").append("\n");
        sb.append("<th style=\"test-align:left\">Table</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Phase<br/>State</th>").append("\n");
        sb.append("<th style=\"test-align:right\">Duration</th>").append("\n");
        sb.append("<th style=\"test-align:right\">Partition<br/>Count</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Actions</th>").append("\n");
        sb.append("<th style=\"test-align:left\">LEFT Table Actions</th>").append("\n");
        sb.append("<th style=\"test-align:left\">RIGHT Table Actions</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Added<br/>Properties</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Issues</th>").append("\n");
        if (config.isSqlOutput()) {
            sb.append("<th style=\"test-align:left\">SQL</th>").append("\n");
        }
        sb.append("</tr>").append("\n");


//        sb.append("|").append(" Table ").append("|")
//                .append("Phase<br/>State").append("|")
//                .append("Phase<br/>Duration").append("|")
//                .append("Partition<br/>Count").append("|")
//                .append("Actions").append("|")
//                .append("LEFT Table Actions").append("|")
//                .append("RIGHT Table Actions").append("|")
//                .append("Added<br/>Properties").append("|")
//                .append("Issues").append("|");
//        if (config.isSqlOutput()) {
//            sb.append("SQL").append("|");
//        }
//        sb.append("\n");

//        sb.append("|").append(":---").append("|")
//                .append(":---").append("|")
//                .append("---:").append("|")
//                .append("---:").append("|")
//                .append(":---").append("|")
//                .append(":---").append("|")
//                .append(":---").append("|")
//                .append(":---").append("|")
//                .append(":---").append("|");
//        if (config.isSqlOutput()) {
//            sb.append(":---").append("|");
//        }
//        sb.append("\n");

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            sb.append("<tr>").append("\n");
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            sb.append("<td>").append(table).append("</td>").append("\n");
            sb.append("<td>").append(tblMirror.getPhaseState().toString()).append("</td>").append("\n");
//            sb.append("|").append(table).append("|")
//                    .append(tblMirror.getPhaseState().toString()).append("|");

            // Stage Duration
            BigDecimal secs = new BigDecimal(tblMirror.getStageDuration()).divide(new BigDecimal(1000));///1000
            DecimalFormat decf = new DecimalFormat("#,###.00");
            String secStr = decf.format(secs);
            sb.append("<td>").append(secStr).append("</td>").append("\n");

//            sb.append(secStr).append(" secs |");

            // Partition Count
            sb.append("<td>").append(tblMirror.getPartitionDefinition(Environment.LEFT) != null ?
                    tblMirror.getPartitionDefinition(Environment.LEFT).size() : " ").append("</td>").append("\n");
//            sb.append(tblMirror.getPartitionDefinition(Environment.LEFT) != null ?
//                    tblMirror.getPartitionDefinition(Environment.LEFT).size() : " ").append("|");

            // Actions
            Iterator<Map.Entry<String[], Object>> aIter = tblMirror.getActions().entrySet().iterator();
            sb.append("<td>").append("\n");
            sb.append("<table>");
            while (aIter.hasNext()) {
                sb.append("<tr>");
                Map.Entry<String[], Object> item = aIter.next();
                String[] keySet = item.getKey();
                sb.append("<td style=\"text-align:left\">").append(keySet[0]).append("</td>");
                sb.append("<td>").append(keySet[1]).append("</td>");
                if (item.getValue() != null)
                    sb.append("<td>").append(item.getValue().toString()).append("</td>");
                else
                    sb.append("<td></td>");
                sb.append("</tr>");
            }
            sb.append("</table>");
            sb.append("</td>").append("\n");
//            sb.append("|");

            // LEFT Table Actions
            Iterator<String> a1Iter = tblMirror.getTableActions(Environment.LEFT).iterator();
            sb.append("<td>").append("\n");
            sb.append("<table>");
            while (a1Iter.hasNext()) {
                sb.append("<tr>");
                String item = a1Iter.next();
                sb.append("<td style=\"text-align:left\">").append(item).append(";</td>");
                sb.append("</tr>");
            }
            sb.append("</table>");
            sb.append("</td>").append("\n");
//            sb.append("|");

            // RIGHT Table Actions
            Iterator<String> a2Iter = tblMirror.getTableActions(Environment.RIGHT).iterator();
            sb.append("<td>").append("\n");
            sb.append("<table>");
            while (a2Iter.hasNext()) {
                sb.append("<tr>");
                String item = a2Iter.next();
                sb.append("<td style=\"text-align:left\">").append(item).append(";</td>");
                sb.append("</tr>");
            }
            sb.append("</table>");
            sb.append("</td>").append("\n");
//            sb.append("|");

            // Properties
            sb.append("<td>").append("\n");
            if (tblMirror.whereTherePropsAdded()) {
                Set<String> keys = tblMirror.getPropAdd().keySet();
                for (String key: keys) {
                    if (tblMirror.getPropAdd().get(key) != null) {
                        sb.append("'").append(key).append("'='").append(tblMirror.getPropAdd().get(key)).append("'<br/>");
                    } else {
                        sb.append("'").append(key).append("'<br/>");
                    }
                }
            } else {
                sb.append(" ");
            }
            sb.append("</td>").append("\n");
//            sb.append("|");
            // Issues
            sb.append("<td>").append("\n");
            if (tblMirror.isThereAnIssue()) {
                sb.append("<ul>");
                for (String issue : tblMirror.getIssues()) {
                    sb.append("<li>").append(issue).append("</li>");
//                        sb.append(append(issue).append("<br/>");
                }
                sb.append("</ul>");
            } else {
                sb.append(" ");
            }
            sb.append("</td>").append("\n");
//            sb.append("|");
            if (config.isSqlOutput()) {
                sb.append("<td>").append("\n");
                // Issues
                if (tblMirror.isThereSql()) {
                    for (String sql : tblMirror.getSql()) {
                        String scrubbedSql = sql.replace("\n", "<br/>");
                        sb.append(scrubbedSql).append(";<br/><br/>");
                    }
                } else {
                    sb.append(" ");
                }
                sb.append("</td>").append("\n");
            }
            sb.append("</tr>").append("\n");
//            sb.append("|\n");
        }
        sb.append("</table>").append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Conversion:").append("\n");
        sb.append("\tDatabases : ").append(databases.size()).append("\n");
        int tables = 0;
        int partitions = 0;
        for (String database : databases.keySet()) {
            DBMirror db = databases.get(database);
            tables += db.getTableMirrors().size();
            for (String table : db.getTableMirrors().keySet()) {
                if (db.getTableMirrors().get(table).getPartitionDefinition(Environment.LEFT) != null) {
                    partitions += db.getTableMirrors().get(table).getPartitionDefinition(Environment.LEFT).size();
                }
            }
        }
        sb.append("\tTables    : " + tables).append("\n");
        sb.append("\tPartitions: " + partitions);

        return sb.toString();
    }
}
