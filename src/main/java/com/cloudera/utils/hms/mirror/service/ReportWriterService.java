/*
 * Copyright (c) 2024. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.MessageCode;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.*;
import com.cloudera.utils.hms.util.UrlUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.commonmark.Extension;
import org.commonmark.ext.front.matter.YamlFrontMatterExtension;
import org.commonmark.ext.gfm.tables.TablesExtension;
import org.commonmark.node.Node;
import org.commonmark.renderer.html.HtmlRenderer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.FileSystems;
import java.text.DecimalFormat;
import java.util.*;

import static java.util.Objects.nonNull;

@Component
@Slf4j
@Getter
@Setter
public class ReportWriterService {

    private DistCpService distCpService;
    private ObjectMapper yamlMapper;
    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private TranslatorService translatorService;

    @Autowired
    public void setDistCpService(DistCpService distCpService) {
        this.distCpService = distCpService;
    }

    @Autowired
    public void setYamlMapper(ObjectMapper yamlMapper) {
        this.yamlMapper = yamlMapper;
    }

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setTranslatorService(TranslatorService translatorService) {
        this.translatorService = translatorService;
    }

    public void wrapup() {
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();
        Conversion conversion = executeSessionService.getSession().getConversion();
        log.info("Wrapping up the Application Workflow");
        log.info("Setting 'running' to FALSE");
//        executeSessionService.getSession().getRunning().set(Boolean.FALSE);

        // Give the underlying threads a chance to finish.
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("Writing out report(s)");
        writeReport();
//        getCliReporter().refresh(Boolean.TRUE);
//        log.trace("==============================");
//        log.trace(conversion.toString());
//        log.trace("==============================");
        Date endTime = new Date();
        DecimalFormat df = new DecimalFormat("#.###");
        df.setRoundingMode(RoundingMode.CEILING);
    }

    public void writeReport() {
        ExecuteSession session = executeSessionService.getSession();
        HmsMirrorConfig config = session.getConfig();
        RunStatus runStatus = session.getRunStatus();
        runStatus.setReportName(session.getSessionId());
//        if (!setupError) {
        Conversion conversion = session.getConversion();

        // Remove the abstract environments from config before reporting output.
        config.getClusters().remove(Environment.TRANSFER);
        config.getClusters().remove(Environment.SHADOW);

//        ObjectMapper yamlMapper;
//        yamlMapper = new ObjectMapper(new YAMLFactory());
//        yamlMapper.enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Check for existing directory. If it exists, increment and check again.
        // We don't want to overwrite existing data.
        File outputDir = new File(config.getOutputDirectory());
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        String sessionOutputDir = session.getSessionId();
        String reportOutputDir = outputDir + File.separator + sessionOutputDir;
        File reportOutputDirFile = new File(reportOutputDir);
        if (!reportOutputDirFile.exists()) {
            reportOutputDirFile.mkdirs();
            runStatus.setReportName(session.getSessionId());
//            config.setOutputDirectory(fullOutputDirFile.getPath());
        } else {
            int i = 1;
            while (true) {
                String checkDir = reportOutputDir + "_" + i;
                File newOutputDir = new File(checkDir);
                if (!newOutputDir.exists()) {
                    runStatus.setReportName(session.getSessionId() + "_" + i);
                    reportOutputDir = checkDir;
//                    config.setOutputDirectory(newOutputDir.getPath());
                    newOutputDir.mkdirs();
                    break;
                }
                i++;
            }
        }

        log.info("Writing CLI report and artifacts to directory: {}", reportOutputDir);

        // Write out the config used to run this session.
        HmsMirrorConfig resolvedConfig = executeSessionService.getSession().getConfig();
        String configOutputFile = reportOutputDir + File.separator + "session-config.yaml";
        try {
            // We need to mask usernames and passwords.
            String yamlStr = yamlMapper.writeValueAsString(resolvedConfig);
            // Mask User/Passwords in Control File
            yamlStr = yamlStr.replaceAll("user:\\s\".*\"", "user: \"*****\"");
            yamlStr = yamlStr.replaceAll("password:\\s\".*\"", "password: \"*****\"");
            FileWriter configFW = new FileWriter(configOutputFile);
            configFW.write(yamlStr);
            configFW.close();
            log.info("Resolved Config 'saved' to: {}", configOutputFile);
        } catch (IOException ioe) {
            log.error("Problem 'writing' resolved config", ioe);
        }

        String runStatusOutputFile = reportOutputDir + File.separator + "run-status.yaml";
        try {
            // We need to mask usernames and passwords.
            String yamlStr = yamlMapper.writeValueAsString(runStatus);

            FileWriter configFW = new FileWriter(runStatusOutputFile);
            configFW.write(yamlStr);
            configFW.close();
            log.info("Run Status 'saved' to: {}", runStatusOutputFile);
            log.info("Run Status 'saved' to: {}", runStatusOutputFile);
        } catch (IOException ioe) {
            log.error("Problem 'writing' run status", ioe);
        }

        for (Map.Entry<String, DBMirror> dbEntry : conversion.getDatabases().entrySet()) {
            String database = HmsMirrorConfigUtil.getResolvedDB(dbEntry.getKey(), config);
            String originalDatabase = dbEntry.getKey();
//            String database = dbEntry.getKey();
//        }
//        for (String database : hmsMirrorConfig.getDatabases()) {

            String dbReportOutputFile = reportOutputDir + File.separator + database + "_hms-mirror";
            String dbLeftExecuteFile = reportOutputDir + File.separator + database + "_LEFT_execute.sql";
            String dbLeftCleanUpFile = reportOutputDir + File.separator + database + "_LEFT_CleanUp_execute.sql";
            String dbRightExecuteFile = reportOutputDir + File.separator + database + "_RIGHT_execute.sql";
            String dbRightCleanUpFile = reportOutputDir + File.separator + database + "_RIGHT_CleanUp_execute.sql";
            String dbRunbookFile = reportOutputDir + File.separator + database + "_runbook.md";

            try {
                // Output directory maps
                boolean dcLeft = Boolean.FALSE;
                boolean dcRight = Boolean.FALSE;

                if (configService.canDeriveDistcpPlan(session)) {
                    distCpService.buildAllDistCpReports(session, reportOutputDir);
                }


                FileWriter runbookFile = new FileWriter(dbRunbookFile);
                runbookFile.write("# Runbook for database: " + database);
                runbookFile.write("\n\nYou'll find the **run report** in the file:\n\n`" + dbReportOutputFile + ".md|html` " +
                        "\n\nThis file includes details about the configuration at the time this was run and the " +
                        "output/actions on each table in the database that was included.\n\n");
                runbookFile.write("## Steps\n\n");
                if (config.isExecute()) {
                    runbookFile.write("Execute was **ON**, so many of the scripts have been run already.  Verify status " +
                            "in the above report.  `distcp` actions (if requested/applicable) need to be run manually. " +
                            "Some cleanup scripts may have been run if no `distcp` actions were requested.\n\n");
                    if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                        if (config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
                            runbookFile.write("Process ran with RIGHT environment 'disconnected'.  All RIGHT scripts will need to be run manually.\n\n");
                        }
                    }
                } else {
                    runbookFile.write("Execute was **OFF**.  All actions will need to be run manually. See below steps.\n\n");
                }
                int step = 1;
                FileWriter reportFile = new FileWriter(dbReportOutputFile + ".md");
                String mdReportStr = conversion.toReport(originalDatabase, getExecuteSessionService());

                File dbYamlFile = new File(dbReportOutputFile + ".yaml");
                FileWriter dbYamlFileWriter = new FileWriter(dbYamlFile);

                DBMirror yamlDb = conversion.getDatabase(database);
                Map<PhaseState, Integer> phaseSummaryMap = yamlDb.getPhaseSummary();
                if (phaseSummaryMap.containsKey(PhaseState.ERROR)) {
                    Integer errCount = phaseSummaryMap.get(PhaseState.ERROR);
                    // TODO: Add to Error Count
//                    rtn += errCount;
                }

                String dbYamlStr = yamlMapper.writeValueAsString(yamlDb);
                try {
                    dbYamlFileWriter.write(dbYamlStr);
                    log.info("Database ({}) yaml 'saved' to: {}", database, dbYamlFile.getPath());
                } catch (IOException ioe) {
                    log.error("Problem 'writing' database yaml", ioe);
                } finally {
                    dbYamlFileWriter.close();
                }

                reportFile.write(mdReportStr);
                reportFile.flush();
                reportFile.close();
                // Convert to HTML
                List<Extension> extensions = Arrays.asList(TablesExtension.create(), YamlFrontMatterExtension.create());

                org.commonmark.parser.Parser parser = org.commonmark.parser.Parser.builder().extensions(extensions).build();
                Node document = parser.parse(mdReportStr);
                HtmlRenderer renderer = HtmlRenderer.builder().extensions(extensions).build();
                String htmlReportStr = renderer.render(document);  // "<p>This is <em>Sparta</em></p>\n"
                reportFile = new FileWriter(dbReportOutputFile + ".html");
                reportFile.write(htmlReportStr);
                reportFile.close();

                log.info("Status Report of 'hms-mirror' is here: {}.md|html", dbReportOutputFile);

                String les = conversion.executeSql(Environment.LEFT, database);
                if (les != null) {
                    FileWriter leftExecOutput = new FileWriter(dbLeftExecuteFile);
                    leftExecOutput.write(les);
                    leftExecOutput.close();
                    log.info("LEFT Execution Script is here: {}", dbLeftExecuteFile);
                    runbookFile.write(step++ + ". **LEFT** clusters SQL script. ");
                    if (config.isExecute()) {
                        runbookFile.write(" (Has been executed already, check report file details)");
                    } else {
                        runbookFile.write("(Has NOT been executed yet)");
                    }
                    runbookFile.write("\n");
                }

                if (dcLeft) {
                    runbookFile.write(step++ + ". **LEFT** cluster `distcp` actions.  Needs to be performed manually.  Use 'distcp' report/template.");
                    runbookFile.write("\n");
                }

                String res = conversion.executeSql(Environment.RIGHT, database);
                if (res != null) {
                    FileWriter rightExecOutput = new FileWriter(dbRightExecuteFile);
                    rightExecOutput.write(res);
                    rightExecOutput.close();
                    log.info("RIGHT Execution Script is here: {}", dbRightExecuteFile);
                    runbookFile.write(step++ + ". **RIGHT** clusters SQL script. ");
                    if (config.isExecute()) {
                        if (!config.getCluster(Environment.RIGHT).getHiveServer2().isDisconnected()) {
                            runbookFile.write(" (Has been executed already, check report file details)");
                        } else {
                            runbookFile.write(" (Has NOT been executed because the environment is NOT connected.  Review and run scripts manually.)");
                        }
                    } else {
                        runbookFile.write("(Has NOT been executed yet)");
                    }
                    runbookFile.write("\n");
                }

                if (dcRight) {
                    runbookFile.write(step++ + ". **RIGHT** cluster `distcp` actions.  Needs to be performed manually.  Use 'distcp' report/template.");
                    runbookFile.write("\n");
                }

                String lcu = conversion.executeCleanUpSql(Environment.LEFT, database, HmsMirrorConfigUtil.getResolvedDB(database, config));
                if (lcu != null) {
                    FileWriter leftCleanUpOutput = new FileWriter(dbLeftCleanUpFile);
                    leftCleanUpOutput.write(lcu);
                    leftCleanUpOutput.close();
                    log.info("LEFT CleanUp Execution Script is here: {}", dbLeftCleanUpFile);
                    runbookFile.write(step++ + ". **LEFT** clusters CLEANUP SQL script. ");
                    runbookFile.write("(Has NOT been executed yet)");
                    runbookFile.write("\n");
                }

                String rcu = conversion.executeCleanUpSql(Environment.RIGHT, database, HmsMirrorConfigUtil.getResolvedDB(database, config));
                if (rcu != null) {
                    FileWriter rightCleanUpOutput = new FileWriter(dbRightCleanUpFile);
                    rightCleanUpOutput.write(rcu);
                    rightCleanUpOutput.close();
                    log.info("RIGHT CleanUp Execution Script is here: {}", dbRightCleanUpFile);
                    runbookFile.write(step++ + ". **RIGHT** clusters CLEANUP SQL script. ");
                    runbookFile.write("(Has NOT been executed yet)");
                    runbookFile.write("\n");
                }
                log.info("Runbook here: {}", dbRunbookFile);
                runbookFile.close();
            } catch (IOException ioe) {
                log.error("Issue writing report for: {}", database, ioe);
            }
        }
//        }

    }
}
