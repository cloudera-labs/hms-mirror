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
        log.trace("==============================");
        log.trace(conversion.toString());
        log.trace("==============================");
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
        } else {
            int i = 1;
            String origOutputDir = config.getOutputDirectory();
            while (true) {
                File newOutputDir = new File(origOutputDir + "_" + i);
                if (!newOutputDir.exists()) {
                    runStatus.setReportName(session.getSessionId() + "_" + i);
                    config.setOutputDirectory(newOutputDir.getPath());
                    outputDir = newOutputDir;
                    newOutputDir.mkdirs();
                    break;
                }
                i++;
            }
        }

        log.info("Writing CLI report and artifacts to directory: {}", config.getOutputDirectory());

        // Write out the config used to run this session.
        HmsMirrorConfig resolvedConfig = executeSessionService.getSession().getConfig();
        String configOutputFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + "session-config.yaml";
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

        String runStatusOutputFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + "run-status.yaml";
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

            String dbReportOutputFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database + "_hms-mirror";
            String dbLeftExecuteFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database + "_LEFT_execute.sql";
            String dbLeftCleanUpFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database + "_LEFT_CleanUp_execute.sql";
            String dbRightExecuteFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database + "_RIGHT_execute.sql";
            String dbRightCleanUpFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database + "_RIGHT_CleanUp_execute.sql";
            String dbRunbookFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database + "_runbook.md";

            try {
                // Output directory maps
                boolean dcLeft = Boolean.FALSE;
                boolean dcRight = Boolean.FALSE;

                if (configService.canDeriveDistcpPlan(session)) {
                    distCpService.buildAllDistCpReports(session);
//                    try {
//                        Environment[] environments = null;
//                        switch (config.getDataStrategy()) {
//
//                            case DUMP:
//                            case STORAGE_MIGRATION:
//                                environments = new Environment[]{Environment.LEFT};
//                                break;
//                            default:
//                                environments = new Environment[]{Environment.LEFT, Environment.RIGHT};
//                                break;
//                        }
//
//                        for (Environment distcpEnv : environments) {
//                            boolean dcFound = Boolean.FALSE;
//
//                            StringBuilder distcpWorkbookSb = new StringBuilder();
//                            StringBuilder distcpScriptSb = new StringBuilder();
//
//                            distcpScriptSb.append("#!/usr/bin/env sh").append("\n");
//                            distcpScriptSb.append("\n");
//                            distcpScriptSb.append("# 1. Copy the source '*_distcp_source.txt' files to the distributed filesystem.").append("\n");
//                            distcpScriptSb.append("# 2. Export an env var 'HCFS_BASE_DIR' that represents where these files where placed.").append("\n");
//                            distcpScriptSb.append("#      NOTE: ${HCFS_BASE_DIR} must be available to the user running 'distcp'").append("\n");
//                            distcpScriptSb.append("# 3. Export an env var 'DISTCP_OPTS' with any special settings needed to run the job.").append("\n");
//                            distcpScriptSb.append("#      For large jobs, you may need to adjust memory settings.").append("\n");
//                            distcpScriptSb.append("# 4. Run the following in an order or framework that is appropriate for your environment.").append("\n");
//                            distcpScriptSb.append("#       These aren't necessarily expected to run in this shell script as is in production.").append("\n");
//                            distcpScriptSb.append("\n");
//                            distcpScriptSb.append("\n");
//                            distcpScriptSb.append("if [ -z ${HCFS_BASE_DIR+x} ]; then").append("\n");
//                            distcpScriptSb.append("  echo \"HCFS_BASE_DIR is unset\"").append("\n");
//                            distcpScriptSb.append("  echo \"What is the 'HCFS_BASE_DIR':\"").append("\n");
//                            distcpScriptSb.append("  read HCFS_BASE_DIR").append("\n");
//                            distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
//                            distcpScriptSb.append("else").append("\n");
//                            distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
//                            distcpScriptSb.append("fi").append("\n");
//                            distcpScriptSb.append("\n");
//                            distcpScriptSb.append("echo \"Creating HCFS directory: $HCFS_BASE_DIR\"").append("\n");
//                            distcpScriptSb.append("hdfs dfs -mkdir -p $HCFS_BASE_DIR").append("\n");
//                            distcpScriptSb.append("\n");
//
//                            // WARNING ABOUT 'distcp' and 'table alignment'
//                            distcpWorkbookSb.append("## WARNING\n");
////                            distcpWorkbookSb.append(MessageCode.RDL_DC_WARNING_TABLE_ALIGNMENT.getDesc()).append("\n\n");
//
//                            distcpWorkbookSb.append("| Database | Target | Sources |\n");
//                            distcpWorkbookSb.append("|:---|:---|:---|\n");
//
//                            FileWriter distcpSourceFW = null;
//
//                            Map<String, Map<String, Set<String>>> distcpPlans = getTranslatorService().buildDistcpList(database, distcpEnv, 1);
//                            if (!distcpPlans.isEmpty()) {
//                                String distcpPlansFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + originalDatabase + "_" + distcpEnv.toString() + "_distcp_plans.yaml";
//                                FileWriter distcpPlansFW = new FileWriter(distcpPlansFile);
//                                String planYaml = yamlMapper.writeValueAsString(distcpPlans);
//                                distcpPlansFW.write(planYaml);
//                                distcpPlansFW.close();
//                            }
//
//                            for (Map.Entry<String, Map<String, Set<String>>> entry : distcpPlans.entrySet()) {
//
//                                distcpWorkbookSb.append("| ").append(entry.getKey()).append(" | | |\n");
//                                Map<String, Set<String>> value = entry.getValue();
//                                int i = 1;
//                                // https://github.com/cloudera-labs/hms-mirror/issues/105
//                                // When there are multiple sources, we need to create a file for each source. BUT, when
//                                // there is only one source, we can skip uses a file and just use the source directly.
//                                // With 'distcp' and the '-f' option when there is only one source the last path element is
//                                //   NOT carried over to the target. When there are multiple sources, the last path element
//                                //   is carried over. Hence, the logic adjustment here to address the behavioral differences.
//                                for (Map.Entry<String, Set<String>> dbMap : value.entrySet()) {
//                                    if (dbMap.getValue().size() > 1) {
//                                        String distcpSourceFile = entry.getKey() + "_" + distcpEnv.toString() + "_" + i++ + "_distcp_source.txt";
//                                        String distcpSourceFileFull = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + distcpSourceFile;
//                                        distcpSourceFW = new FileWriter(distcpSourceFileFull);
//
//                                        StringBuilder line = new StringBuilder();
//                                        line.append("| | ").append(dbMap.getKey()).append(" | ");
//
//                                        for (String source : dbMap.getValue()) {
//                                            line.append(source).append("<br>");
//                                            distcpSourceFW.append(source).append("\n");
//                                        }
//                                        line.append(" | ").append("\n");
//                                        distcpWorkbookSb.append(line);
//
//                                        distcpScriptSb.append("\n");
//                                        distcpScriptSb.append("echo \"Copying 'distcp' source file to $HCFS_BASE_DIR\"").append("\n");
//                                        distcpScriptSb.append("\n");
//                                        distcpScriptSb.append("hdfs dfs -copyFromLocal -f ").append(distcpSourceFile).append(" ${HCFS_BASE_DIR}").append("\n");
//                                        distcpScriptSb.append("\n");
//                                        distcpScriptSb.append("echo \"Running 'distcp'\"").append("\n");
//                                        distcpScriptSb.append("hadoop distcp ${DISTCP_OPTS} -f ${HCFS_BASE_DIR}/").append(distcpSourceFile).append(" ").append(dbMap.getKey()).append("\n").append("\n");
//
//                                        distcpSourceFW.close();
//                                    } else {
//                                        // Only 1 entry, so we can skip the file and just use the source directly.
//                                        String source = dbMap.getValue().iterator().next();
//                                        // Get last path element
//                                        String lastPathElement = UrlUtils.getLastDirFromUrl(source);//).substring(source.lastIndexOf("/") + 1);
//
//                                        distcpScriptSb.append("echo \"Only one element in path.\"").append("\n");
//
//                                        String target = dbMap.getKey();
//
//                                        // Concatenate the last path element to the target
//                                        if (!target.endsWith("/") && !lastPathElement.startsWith("/")) {
//                                            target += "/" + lastPathElement;
//                                        } else if (target.endsWith("/") && lastPathElement.startsWith("/")) {
//                                            target += lastPathElement.substring(1);
//                                        } else {
//                                            target += lastPathElement;
//                                        }
//
//                                        StringBuilder line = new StringBuilder();
//                                        line.append("| | ").append(target).append(" | ");
//
//                                        line.append(source).append(" |\n");
//
//                                        distcpWorkbookSb.append(line);
//
//                                        distcpScriptSb.append("echo \"Running 'distcp'\"").append("\n");
//                                        distcpScriptSb.append("hadoop distcp ${DISTCP_OPTS} ").append(source).append(" ").append(target).append("\n").append("\n");
//                                    }
//
//                                    dcFound = Boolean.TRUE;
//                                }
//                            }
//
//                            if (dcFound) {
//                                // Set flags for report and workplan
//                                switch (distcpEnv) {
//                                    case LEFT:
//                                        dcLeft = Boolean.TRUE;
//                                        break;
//                                    case RIGHT:
//                                        dcRight = Boolean.TRUE;
//                                        break;
//                                }
//
//                                String distcpWorkbookFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database +
//                                        "_" + distcpEnv + "_distcp_workbook.md";
//                                String distcpScriptFile = config.getOutputDirectory() + FileSystems.getDefault().getSeparator() + database +
//                                        "_" + distcpEnv + "_distcp_script.sh";
//
//                                FileWriter distcpWorkbookFW = new FileWriter(distcpWorkbookFile);
//                                FileWriter distcpScriptFW = new FileWriter(distcpScriptFile);
//
//                                distcpScriptFW.write(distcpScriptSb.toString());
//                                distcpWorkbookFW.write(distcpWorkbookSb.toString());
//
//                                distcpScriptFW.close();
//                                distcpWorkbookFW.close();
//                            }
//                        }
//                    } catch (IOException ioe) {
//                        log.error("Issue writing distcp workbook", ioe);
//                    }
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
                    if (nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
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
