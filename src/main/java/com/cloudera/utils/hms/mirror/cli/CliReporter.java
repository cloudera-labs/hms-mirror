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

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.service.HmsMirrorCfgService;
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
import java.util.*;

@Component
@Getter
@Setter
@Slf4j
public class CliReporter {
    private final Date start = new Date();
//    private final AtomicBoolean running = new AtomicBoolean(false);
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
    private String reportOutputFile = null;
    private String leftExecuteFile = null;
    private String leftCleanUpFile = null;
    private String rightExecuteFile = null;
    private String rightCleanUpFile = null;
    private HmsMirrorCfgService hmsMirrorCfgService;
    private Conversion conversion;
    private boolean tiktok = false;

    @Bean
    @Order(20)
    CommandLineRunner configQuiet(HmsMirrorConfig hmsMirrorConfig) {
        return args -> {
            setQuiet(hmsMirrorConfig.isQuiet());
        };
    }

    protected void displayReport(Boolean showAll) {
        System.out.print(ReportingConf.CLEAR_CONSOLE);
        StringBuilder report = new StringBuilder();
        // Header
        if (!quiet) {
            report.append(ReportingConf.substituteAllVariables(reportTemplateHeader, varMap));

            // Table Processing
            for (TableMirror tblMirror : startedTables) {
                Map<String, String> tblVars = new TreeMap<>();
                tblVars.put("db.name", getHmsMirrorCfgService().getResolvedDB(tblMirror.getParent().getName()));
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

            Progression progression = hmsMirrorCfgService.getHmsMirrorConfig().getProgression();

            if (progression.getErrors().getMessages().length > 0) {
                report.append("\n=== Errors ===\n");
                for (String message : progression.getErrors().getMessages()) {
                    report.append("\t").append(message).append("\n");
                }
            }

            if (progression.getWarnings().getMessages().length > 0) {
                report.append("\n=== Warnings ===\n");
                for (String message : progression.getWarnings().getMessages()) {
                    report.append("\t").append(message).append("\n");
                }
            }
        }

        System.out.print(report);

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
        tiktok = !tiktok;
        startedTables.clear();
        if (!retry)
            varMap.put("retry", "       ");
        else
            varMap.put("retry", "(RETRY)");
        varMap.put("run.mode", getHmsMirrorCfgService().getHmsMirrorConfig().isExecute() ? "EXECUTE" : "DRYRUN");
        varMap.put("HMS-Mirror-Version", ReportingConf.substituteVariablesFromManifest("${HMS-Mirror-Version}"));
        varMap.put("config.file", getHmsMirrorCfgService().getHmsMirrorConfig().getConfigFilename());
        varMap.put("config.strategy", getHmsMirrorCfgService().getHmsMirrorConfig().getDataStrategy().toString());
        varMap.put("tik.tok", tiktok ? "*" : "");
        varMap.put("java.version", System.getProperty("java.version"));
        varMap.put("os.name", System.getProperty("os.name"));
        varMap.put("cores", Integer.toString(Runtime.getRuntime().availableProcessors()));
        varMap.put("os.arch", System.getProperty("os.arch"));
        varMap.put("memory", Runtime.getRuntime().totalMemory() / 1024 / 1024 + "MB");

        varMap.put("report.file", this.getReportOutputFile());
        varMap.put("left.execute.file", this.getLeftExecuteFile());
        varMap.put("left.cleanup.file", this.getLeftCleanUpFile());
        varMap.put("right.execute.file", this.getRightExecuteFile());
        varMap.put("right.cleanup.file", this.getRightCleanUpFile());

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
                    case STARTED:
                        started++;
                        startedTables.add(dbMirror.getTable(tbl));
                        break;
                    case SUCCESS:
                        completed++;
                        break;
                    case ERROR:
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
        try {
            fetchReportTemplates();
            log.info("Starting Reporting Thread");
            getHmsMirrorCfgService().getRunning().set(true);
            while (getHmsMirrorCfgService().getRunning().get()) {
                refresh(Boolean.FALSE);
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
//                    e.printStackTrace();
                    System.out.println("Reporting thread interrupted");
                }
            }
            // Final Screen Report.  Write the location out to the console.
//            refresh(Boolean.TRUE);

            log.info("Completed Reporting Thread");
        } catch (IOException ioe) {
            System.out.println("Missing Reporting Template");
        }
    }

    @Autowired
    public void setHmsMirrorCfgService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    @Autowired
    public void setConversion(Conversion conversion) {
        this.conversion = conversion;
    }

    public void setVariable(String key, String value) {
        varMap.put(key, value);
    }

}
