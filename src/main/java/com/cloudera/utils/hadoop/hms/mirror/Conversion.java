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

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.*;

public class Conversion {
    @JsonIgnore
    private final Date start = new Date();
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

    public String executeSql(Environment environment, String database) {
        StringBuilder sb = new StringBuilder();
        Boolean found = Boolean.FALSE;
        sb.append("-- EXECUTION script for ").append(database).append(" on ").append(environment).append(" cluster\n\n");
        sb.append("-- ").append(new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()));
        sb.append("-- These are the command run on the " + environment + " cluster when `-e` is used.\n");
        DBMirror dbMirror = databases.get(database);

        List<Pair> dbSql = dbMirror.getSql(environment);
        if (dbSql != null && dbSql.size() > 0) {
            for (Pair sqlPair : dbSql) {
                sb.append("-- ").append(sqlPair.getDescription()).append("\n");
                sb.append(sqlPair.getAction()).append(";\n");
            }
        }

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            sb.append("\n--    Table: ").append(table).append("\n");
            if (tblMirror.isThereSql(environment)) {
                for (Pair pair : tblMirror.getSql(environment)) {
                    sb.append(pair.getAction()).append(";\n");
                    found = Boolean.TRUE;
                }
            } else {
                sb.append("\n");
            }
        }
        if (found)
            return sb.toString();
        else
            return null;
    }

    public String executeCleanUpSql(Environment environment, String database) {
        StringBuilder sb = new StringBuilder();
        Boolean found = Boolean.FALSE;
        sb.append("-- EXECUTION CLEANUP script for ").append(database).append(" on ").append(environment).append(" cluster\n\n");
        sb.append("-- ").append(new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date())).append("\n\n");
//        sb.append("-- These are the command run on the " + environment + " cluster when `-e` is used.\n");
        String rDb = config.getResolvedDB(database);
        DBMirror dbMirror = databases.get(database);

        sb.append("USE ").append(rDb).append(";\n");

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            if (tblMirror.isThereCleanupSql(environment)) {
                sb.append("\n--    Cleanup script: ").append(table).append("\n");
                for (Pair pair : tblMirror.getCleanUpSql(environment)) {
                    sb.append(pair.getAction());
                    // Skip ';' when its a comment
                    // https://github.com/cloudera-labs/hms-mirror/issues/33
                    if (!pair.getAction().trim().startsWith("--")) {
                        sb.append(";\n");
                        found = Boolean.TRUE;
                    }
                }
            } else {
                sb.append("\n");
            }
        }
        if (found)
            return sb.toString();
        else
            return null;
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
        DecimalFormat lngdecf = new DecimalFormat("#,###");
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

        if (config.getErrors().getMessages().length > 0) {
            sb.append("### Config Errors:\n");
            for (String message : config.getErrors().getMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }
        if (config.getWarnings().getMessages().length > 0) {
            sb.append("### Config Warnings:\n");
            for (String message : config.getWarnings().getMessages()) {
                sb.append("- ").append(message).append("\n");
            }
            sb.append("\n");
        }

        DBMirror dbMirror = databases.get(database);

        sb.append("## Database SQL Statement(s)").append("\n\n");

        for (Environment environment : Environment.values()) {
            if (dbMirror.getSql(environment) != null && dbMirror.getSql(environment).size() > 0) {
                sb.append("### ").append(environment.toString()).append("\n\n");
                sb.append("```\n");
                for (Pair sqlPair : dbMirror.getSql(environment)) {
                    sb.append("-- ").append(sqlPair.getDescription()).append("\n");
                    sb.append(sqlPair.getAction()).append("\n");
                }
                sb.append("```\n");
            }
        }

        sb.append("\n");

        sb.append("## DB Issues").append("\n\n");
        // Issues
        if (dbMirror.isThereAnIssue()) {
            Set<Environment> environments = Sets.newHashSet(Environment.LEFT, Environment.RIGHT);
            for (Environment env : environments) {
                List<String> issues = dbMirror.getIssuesList(env);
                if (issues != null) {
                    sb.append("### ").append(env).append("\n\n");
                    for (String issue : issues) {
                        sb.append("* ").append(issue).append("\n");
                    }
                }
            }
        } else {
            sb.append("none\n");
        }

        sb.append("\n## Table Status (").append(dbMirror.getTableMirrors().size()).append(")  ");
        sb.append(dbMirror.getPhaseSummaryString()).append("\n\n");

        sb.append("*NOTE* SQL in this report may be altered by the renderer.  Do NOT COPY/PASTE from this report.  Use the LEFT|RIGHT_execution.sql files for accurate scripts\n\n");

        sb.append("<table>").append("\n");
        sb.append("<tr>").append("\n");
        sb.append("<th style=\"test-align:left\">Table</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Strategy</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Source<br/>Managed</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Source<br/>ACID</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Phase<br/>State</th>").append("\n");
        sb.append("<th style=\"test-align:right\">Duration</th>").append("\n");
//        sb.append("<th style=\"test-align:right\">Partition<br/>Count</th>").append("\n");
        sb.append("<th style=\"test-align:left\">Steps</th>").append("\n");
        if (dbMirror.hasActions()) {
            sb.append("<th style=\"test-align:left\">Actions</th>").append("\n");
        }
        if (dbMirror.hasAddedProperties()) {
            sb.append("<th style=\"test-align:left\">Added<br/>Properties</th>").append("\n");
        }
        if (dbMirror.hasStatistics()) {
            sb.append("<th style=\"test-align:left\">Stats</th>").append("\n");
        }
        if (dbMirror.hasIssues()) {
            sb.append("<th style=\"test-align:left\">Issues</th>").append("\n");
        }
        sb.append("<th style=\"test-align:left\">SQL</th>").append("\n");
        sb.append("</tr>").append("\n");

        Set<String> tables = dbMirror.getTableMirrors().keySet();
        for (String table : tables) {
            sb.append("<tr>").append("\n");
            TableMirror tblMirror = dbMirror.getTableMirrors().get(table);
            EnvironmentTable let = tblMirror.getEnvironmentTable(Environment.LEFT);
            // table
            sb.append("<td>").append(table).append("</td>").append("\n");
            // Strategy
            sb.append("<td>").append(tblMirror.getStrategy()).append("</td>").append("\n");
            // Source Managed
            sb.append("<td>");
            if (TableUtils.isManaged(let)) {
                sb.append("X");
            }
            sb.append("</td>").append("\n");
            // Source ACID
            sb.append("<td>").append("\n");
            if (TableUtils.isACID(let)) {
                sb.append("X");
            }
            sb.append("</td>").append("\n");
            // phase state
            sb.append("<td>").append(tblMirror.getPhaseState().toString()).append("</td>").append("\n");

            // Stage Duration
            BigDecimal secs = new BigDecimal(tblMirror.getStageDuration()).divide(new BigDecimal(1000));///1000
            DecimalFormat decf = new DecimalFormat("#,###.00");
            String secStr = decf.format(secs);
            sb.append("<td>").append(secStr).append("</td>").append("\n");

            // Partition Count
//            sb.append("<td>").append(let.getPartitioned() ?
//                    let.getPartitions().size() : " ").append("</td>").append("\n");

            // Steps
            sb.append("<td>\n");
            sb.append("<table>\n");
            for (Marker entry : tblMirror.getSteps()) {
                sb.append("<tr>\n");
                sb.append("<td>");
                sb.append(entry.getMark());
                sb.append("</td>");
                sb.append("<td>");
                sb.append(entry.getDescription());
                sb.append("</td>");
                sb.append("<td>");
                if (entry.getAction() != null)
                    sb.append(entry.getAction());
                sb.append("</td>");
                sb.append("</tr>\n");
            }
            sb.append("</table>\n");
            sb.append("</td>\n");

            // Actions
            if (dbMirror.hasActions()) {
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
            }

            // Properties
            if (dbMirror.hasAddedProperties()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (entry.getValue().getAddProperties().size() > 0) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (Map.Entry<String, String> prop : entry.getValue().getAddProperties().entrySet()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(prop.getKey());
                            sb.append("</td>\n");
                            sb.append("<td>");
                            sb.append(prop.getValue());
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // Statistics
            if (dbMirror.hasStatistics()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (entry.getValue().getStatistics().size() > 0) {
                        sb.append("<tr>\n");
                        sb.append("<th colspan=\"2\">");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (Map.Entry<String, Object> prop : entry.getValue().getStatistics().entrySet()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(prop.getKey());
                            sb.append("</td>\n");
                            sb.append("<td>");
                            if (prop.getValue() instanceof Double || prop.getValue() instanceof Long) {
                                sb.append(lngdecf.format(prop.getValue()));
                            } else {
                                sb.append(prop.getValue().toString());
                            }
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                        if (entry.getValue().getPartitioned()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(MirrorConf.PARTITION_COUNT);
                            sb.append("</td>\n");
                            sb.append("<td>");
                            sb.append(entry.getValue().getPartitions().size());
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }
            // Issues Reporting
            if (dbMirror.hasIssues()) {
                sb.append("<td>").append("\n");
                sb.append("<table>");
                for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                    if (entry.getValue().getIssues().size() > 0) {
                        sb.append("<tr>\n");
                        sb.append("<th>");
                        sb.append(entry.getKey());
                        sb.append("</th>\n");
                        sb.append("</tr>").append("\n");

                        for (String issue : entry.getValue().getIssues()) {
                            sb.append("<tr>\n");
                            sb.append("<td>");
                            sb.append(issue);
                            sb.append("</td>\n");
                            sb.append("</tr>\n");
                        }
                    }
                }
                sb.append("</table>");
                sb.append("</td>").append("\n");
            }

            // SQL Output
            sb.append("<td>\n");
            sb.append("<table>");
            for (Map.Entry<Environment, EnvironmentTable> entry : tblMirror.getEnvironments().entrySet()) {
                if (entry.getValue().getSql().size() > 0) {
                    sb.append("<tr>\n");
                    sb.append("<th colspan=\"2\">");
                    sb.append(entry.getKey());
                    sb.append("</th>\n");
                    sb.append("</tr>").append("\n");

                    for (Pair pair : entry.getValue().getSql()) {
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append(pair.getDescription());
                        sb.append("</td>\n");
                        sb.append("<td>");
                        sb.append(pair.getAction());
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                    for (Pair pair : entry.getValue().getCleanUpSql()) {
                        sb.append("<tr>\n");
                        sb.append("<td>");
                        sb.append(pair.getDescription());
                        sb.append("</td>\n");
                        sb.append("<td>");
                        sb.append(pair.getAction());
                        sb.append("</td>\n");
                        sb.append("</tr>\n");
                    }
                }
            }
            sb.append("</table>");
            sb.append("</td>").append("\n");
            sb.append("</tr>").append("\n");
        }
        sb.append("</table>").append("\n");

        if (dbMirror.getFilteredOut().size() > 0) {
            sb.append("\n## Skipped Tables/Views\n\n");

            sb.append("| Table / View | Reason |\n");
            sb.append("|:---|:---|\n");
            for (Map.Entry<String, String> entry : dbMirror.getFilteredOut().entrySet()) {
                sb.append("| ").append(entry.getKey()).append(" | ").append(entry.getValue()).append(" |\n");
            }
        }
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
