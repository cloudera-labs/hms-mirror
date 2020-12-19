package com.streever.hadoop.hms.mirror;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Reporter implements Runnable {

    private Thread worker;
    private Date start = new Date();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private int sleepInterval;
    private List<String> reportTemplateHeader = new ArrayList<String>();
    private List<String> reportTemplateTableDetail = new ArrayList<String>();
    private List<String> reportTemplateFooter = new ArrayList<String>();
    private Map<String, String> varMap = new TreeMap<String, String>();

    private List<TableMirror> startedTables = new ArrayList<TableMirror>();

    private Conversion conversion;

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
        InputStream his = this.getClass().getResourceAsStream("/report_header.txt");
        BufferedReader hbr = new BufferedReader(new InputStreamReader(his));
        String hline = null;
        while ((hline = hbr.readLine()) != null) {
            reportTemplateHeader.add(hline);
        }
        InputStream fis = this.getClass().getResourceAsStream("/report_footer.txt");
        BufferedReader fbr = new BufferedReader(new InputStreamReader(fis));
        String fline = null;
        while ((fline = fbr.readLine()) != null) {
            reportTemplateFooter.add(fline);
        }
        InputStream tis = this.getClass().getResourceAsStream("/table_display.txt");
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
                refresh();
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

    public void refresh() {
        populateVarMap();
        displayReport();
    }

    private boolean tiktok = false;

    /*
    Go through the Conversion object and set the variables.
     */
    private void populateVarMap() {
        tiktok = !tiktok;
        startedTables.clear();
        varMap.put("tik.tok", tiktok?"*":"");
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
        for (String database : conversion.getDatabases().keySet()) {
            DBMirror dbMirror = conversion.getDatabase(database);
            for (String tbl: dbMirror.getTableMirrors().keySet()) {
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
                }
            }
        }
        varMap.put("started.tbls", Integer.toString(started));
        varMap.put("completed.tbls", Integer.toString(completed));
        varMap.put("error.tbls", Integer.toString(errors));
        Date current = new Date();
        long elapsedMS = current.getTime() - start.getTime();
        if (tiktok)
            varMap.put("elapsed.time", "\u001B[34m" + Long.toString(elapsedMS/1000) + "[0m");
        else
            varMap.put("elapsed.time", "\u001B[33m" + Long.toString(elapsedMS/1000) + "[0m");
    }


    protected void displayReport() {
        System.out.print(ReportingConf.CLEAR_CONSOLE);
        StringBuilder report = new StringBuilder();
        // Header
        report.append(ReportingConf.substituteAllVariables(reportTemplateHeader, varMap));

        // Table Processing
        for (TableMirror tblMirror: startedTables) {
            Map<String,String> tblVars = new TreeMap<String, String>();
            tblVars.put("db.name", tblMirror.getDatabase().getDatabase());
            tblVars.put("tbl.name", tblMirror.getName());
            tblVars.put("tbl.progress", tblMirror.getProgressIndicator(80,10));
            tblVars.put("tbl.msg", tblMirror.getMigrationStageMessage());
            tblVars.put("tbl.strategy", tblMirror.getStrategy().toString());
            report.append(ReportingConf.substituteAllVariables(reportTemplateTableDetail, tblVars));
        }

        // Footer
        report.append(ReportingConf.substituteAllVariables(reportTemplateFooter, varMap));

        System.out.println(report.toString());

    }

}
