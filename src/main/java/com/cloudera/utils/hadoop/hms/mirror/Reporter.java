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

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Reporter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Reporter.class);
    private Thread worker;
    private Boolean retry = Boolean.FALSE;
    private Boolean quiet = Boolean.FALSE;
    private final Date start = new Date();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final int sleepInterval;
    private final List<String> reportTemplateHeader = new ArrayList<String>();
    private final List<String> reportTemplateTableDetail = new ArrayList<String>();
    private final List<String> reportTemplateFooter = new ArrayList<String>();
    private final List<String> reportTemplateOutput = new ArrayList<String>();
    private final Map<String, String> varMap = new TreeMap<String, String>();

    private final List<TableMirror> startedTables = new ArrayList<TableMirror>();

    private final Conversion conversion;

    public Boolean getRetry() {
        return retry;
    }

    public void setRetry(Boolean retry) {
        this.retry = retry;
    }

    public Boolean getQuiet() {
        return quiet;
    }

    public void setQuiet(Boolean quiet) {
        this.quiet = quiet;
    }

    public Reporter(Conversion conversion, int sleepInterval) {
        this.conversion = conversion;
        this.sleepInterval = sleepInterval;
    }

    public void start() {
        worker = new Thread(this);
        worker.start();
    }

    public void stop() {
        running.set(false);
    }

    public void setVariable(String key, String value) {
        varMap.put(key, value);
    }

    private void fetchReportTemplates() throws IOException {

        InputStream his = this.getClass().getResourceAsStream(!quiet?"/report_header.txt":"/quiet/report_header.txt");
        BufferedReader hbr = new BufferedReader(new InputStreamReader(his));
        String hline = null;
        while ((hline = hbr.readLine()) != null) {
            reportTemplateHeader.add(hline);
        }
        InputStream fis = this.getClass().getResourceAsStream(!quiet?"/report_footer.txt":"/quiet/report_footer.txt");
        BufferedReader fbr = new BufferedReader(new InputStreamReader(fis));
        String fline = null;
        while ((fline = fbr.readLine()) != null) {
            reportTemplateFooter.add(fline);
        }
        InputStream fisop = this.getClass().getResourceAsStream(!quiet?"/report_output.txt":"/quiet/report_output.txt");
        BufferedReader fbrop = new BufferedReader(new InputStreamReader(fisop));
        String flineop = null;
        while ((flineop = fbrop.readLine()) != null) {
            reportTemplateOutput.add(flineop);
        }
        InputStream tis = this.getClass().getResourceAsStream(!quiet?"/table_display.txt":"/quiet/table_display.txt");
        BufferedReader tbr = new BufferedReader(new InputStreamReader(tis));
        String tline = null;
        while ((tline = tbr.readLine()) != null) {
            reportTemplateTableDetail.add(tline);
        }

    }

    @Override
    public void run() {
        try {
            fetchReportTemplates();
            running.set(true);
            while (running.get()) {
                refresh(Boolean.FALSE);
                try {
                    Thread.sleep(sleepInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("Reporting thread interrupted");
                }
            }
        } catch (IOException ioe) {
            System.out.println("Missing Reporting Template");
        }
    }

    public void refresh(Boolean showAll) {
        try {
            populateVarMap();
            displayReport(showAll);
        } catch (ConcurrentModificationException cme) {
            LOG.error("Report Refresh", cme);
        }
    }

    private boolean tiktok = false;

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
        varMap.put("tik.tok", tiktok ? "*" : "");
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


    protected void displayReport(Boolean showAll) {
        if (Context.getInstance().getInitializing()) {
            return;
        }
        System.out.print(ReportingConf.CLEAR_CONSOLE);
        StringBuilder report = new StringBuilder();
        // Header
        if (!quiet) {
            report.append(ReportingConf.substituteAllVariables(reportTemplateHeader, varMap));

            // Table Processing
            for (TableMirror tblMirror : startedTables) {
                Map<String, String> tblVars = new TreeMap<String, String>();
                tblVars.put("db.name", tblMirror.getParent().getResolvedName());
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
            LOG.info(report.toString());
        }

        System.out.print(report);

    }

}
