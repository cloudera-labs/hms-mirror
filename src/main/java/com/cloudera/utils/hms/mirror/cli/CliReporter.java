/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.cli;

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.reporting.ReportingConf;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Component
@Getter
@Setter
@Slf4j
public class CliReporter {
    private final Date start = new Date();
    private final int sleepInterval = 1000;
    private final List<String> reportTemplateHeader = new ArrayList<>();
    private final List<String> reportTemplateTableDetail = new ArrayList<>();
    private final List<String> reportTemplateFooter = new ArrayList<>();
    private final List<String> reportTemplateOutput = new ArrayList<>();
    private final Map<String, String> varMap = new TreeMap<>();
    private final List<TableMirror> startedTables = new ArrayList<>();
    private Thread worker;
    private Boolean retry = Boolean.FALSE;
    private Boolean quiet = Boolean.FALSE;

    private ConfigService configService;
    private ExecuteSessionService executeSessionService;

    private boolean tiktok = false;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }


    @Bean
    @Order(20)
    CommandLineRunner configQuiet(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            setQuiet(hmsMirrorConfig.isQuiet());
        };
    }

    protected void displayReport(Boolean showAll) {
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        Conversion conversion = session.getConversion();

        System.out.print(ReportingConf.CLEAR_CONSOLE);
        StringBuilder report = new StringBuilder();
        // Header
        if (!quiet) {
            report.append(ReportingConf.substituteAllVariables(reportTemplateHeader, varMap));

            // Table Processing
            for (TableMirror tblMirror : startedTables) {
                Map<String, String> tblVars = new TreeMap<>();
                tblVars.put("db.name", HmsMirrorConfigUtil.getResolvedDB(tblMirror.getParent().getName(), config));
                tblVars.put("tbl.name", tblMirror.getName());
                tblVars.put("tbl.progress", tblMirror.getProgressIndicator(80));
                tblVars.put("tbl.msg", tblMirror.getMigrationStageMessage());
                tblVars.put("tbl.strategy", tblMirror.getStrategy().toString());
                report.append(ReportingConf.substituteAllVariables(reportTemplateTableDetail, tblVars));
            }
        }

        // Footer
        report.append(ReportingConf.substituteAllVariables(reportTemplateFooter, varMap));

        // Output
        if (showAll) {
            report.append("\nDatabases(<db>):\n");
            report.append(String.join(",", conversion.getDatabases().keySet()));
            report.append("\n");
            report.append(ReportingConf.substituteAllVariables(reportTemplateOutput, varMap));
            log.info(report.toString());

            report.append(getMessages());

        }

        System.out.print(report);

    }

    public String getMessages() {
        StringBuilder report = new StringBuilder();
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        if (runStatus.hasErrors()) {
            report.append("\n=== Errors ===\n");
            for (String message : runStatus.getErrorMessages()) {
                report.append("\t").append(message).append("\n");
            }
        }

        if (runStatus.hasWarnings() && !config.isSuppressCliWarnings()) {
            report.append("\n=== Warnings ===\n");
            for (String message : runStatus.getWarningMessages()) {
                report.append("\t").append(message).append("\n");
            }
        }

        return report.toString();
    }

    private void fetchReportTemplates() throws IOException {

        InputStream his = this.getClass().getResourceAsStream(!quiet ? "/report_header.txt" : "/quiet/report_header.txt");
        BufferedReader hbr = new BufferedReader(new InputStreamReader(his));
        String hline = null;
        while ((hline = hbr.readLine()) != null) {
            reportTemplateHeader.add(hline);
        }
        InputStream fis = this.getClass().getResourceAsStream(!quiet ? "/report_footer.txt" : "/quiet/report_footer.txt");
        BufferedReader fbr = new BufferedReader(new InputStreamReader(fis));
        String fline = null;
        while ((fline = fbr.readLine()) != null) {
            reportTemplateFooter.add(fline);
        }
        InputStream fisop = this.getClass().getResourceAsStream(!quiet ? "/report_output.txt" : "/quiet/report_output.txt");
        BufferedReader fbrop = new BufferedReader(new InputStreamReader(fisop));
        String flineop = null;
        while ((flineop = fbrop.readLine()) != null) {
            reportTemplateOutput.add(flineop);
        }
        InputStream tis = this.getClass().getResourceAsStream(!quiet ? "/table_display.txt" : "/quiet/table_display.txt");
        BufferedReader tbr = new BufferedReader(new InputStreamReader(tis));
        String tline = null;
        while ((tline = tbr.readLine()) != null) {
            reportTemplateTableDetail.add(tline);
        }

    }

    /*
    Go through the Conversion object and set the variables.
     */
    private void populateVarMap() {
        ExecuteSession session = executeSessionService.getSession();
        Conversion conversion = executeSessionService.getSession().getConversion();
        HmsMirrorConfig config = session.getConfig();

        tiktok = !tiktok;
        startedTables.clear();
        if (!retry)
            varMap.put("retry", "       ");
        else
            varMap.put("retry", "(RETRY)");
        varMap.put("run.mode", config.isExecute() ? "EXECUTE" : "DRYRUN");
        varMap.put("HMS-Mirror-Version", ReportingConf.substituteVariablesFromManifest("${HMS-Mirror-Version}"));
        varMap.put("config.file", config.getConfigFilename());
        varMap.put("config.strategy", config.getDataStrategy().toString());
        varMap.put("tik.tok", tiktok ? "*" : "");
        varMap.put("java.version", System.getProperty("java.version"));
        varMap.put("os.name", System.getProperty("os.name"));
        varMap.put("cores", Integer.toString(Runtime.getRuntime().availableProcessors()));
        varMap.put("os.arch", System.getProperty("os.arch"));
        varMap.put("memory", Runtime.getRuntime().totalMemory() / 1024 / 1024 + "MB");

        String outputDir = executeSessionService.getSession().getConfig().getOutputDirectory();
        if (!isBlank(executeSessionService.getSession().getConfig().getFinalOutputDirectory())) {
            outputDir = executeSessionService.getSession().getConfig().getFinalOutputDirectory();
        }

        varMap.put("report.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_hms-mirror.md|html|yaml");
        varMap.put("left.execute.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_LEFT_execute.sql");

        varMap.put("left.cleanup.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_LEFT_CleanUp_execute.sql");
        varMap.put("right.execute.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_RIGHT_execute.sql");
        varMap.put("right.cleanup.file", outputDir + FileSystems.getDefault().getSeparator() + "<db>_RIGHT_CleanUp_execute.sql");

        varMap.put("total.dbs", Integer.toString(conversion.getDatabases().size()));
        // Count
        int tblCount = 0;
        for (String database : conversion.getDatabases().keySet()) {
            tblCount += conversion.getDatabase(database).getTableMirrors().size();
        }
        varMap.put("total.tbls", Integer.toString(tblCount));

        // Table Counters
        int started = 0;
        int completed = 0;
        int errors = 0;
        int skipped = 0;
        for (String database : conversion.getDatabases().keySet()) {
            DBMirror dbMirror = conversion.getDatabase(database);
            for (String tbl : dbMirror.getTableMirrors().keySet()) {
                switch (dbMirror.getTable(tbl).getPhaseState()) {
                    case INIT:
                        break;
                    case APPLYING_SQL:
                    case CALCULATING_SQL:
                        started++;
                        startedTables.add(dbMirror.getTable(tbl));
                        break;
                    case CALCULATED_SQL:
                        if (config.isExecute())
                            started++;
                        else
                            completed++;
                        break;
                    case PROCESSED:
                        completed++;
                        break;
                    case ERROR:
                    case CALCULATED_SQL_WARNING:
                        errors++;
                        break;
                    case RETRY_SKIPPED_PAST_SUCCESS:
                        skipped++;
                }
            }
        }
        varMap.put("started.tbls", Integer.toString(started));
        varMap.put("completed.tbls", Integer.toString(completed));
        varMap.put("error.tbls", Integer.toString(errors));
        varMap.put("skipped.tbls", Integer.toString(skipped));
        Date current = new Date();
        long elapsedMS = current.getTime() - start.getTime();
        if (tiktok)
            varMap.put("elapsed.time", "\u001B[34m" + elapsedMS / 1000 + "[0m");
        else
            varMap.put("elapsed.time", "\u001B[33m" + elapsedMS / 1000 + "[0m");
    }

    public void refresh(Boolean showAll) {
        try {
            populateVarMap();
            displayReport(showAll);
        } catch (ConcurrentModificationException cme) {
            log.error("Report Refresh", cme);
        }
    }

    @Async("reportingThreadPool")
    public void run() {
        ExecuteSession session = executeSessionService.getSession();
        try {
            fetchReportTemplates();
            log.info("Starting Reporting Thread");
            // Wait for the main thread to start.
            while (!session.isRunning()) {
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            while (session.isRunning()) {
                refresh(Boolean.FALSE);
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("Completed Reporting Thread");
        } catch (IOException ioe) {
            System.out.println("Missing Reporting Template");
        }
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    public void setVariable(String key, String value) {
        varMap.put(key, value);
    }

}
