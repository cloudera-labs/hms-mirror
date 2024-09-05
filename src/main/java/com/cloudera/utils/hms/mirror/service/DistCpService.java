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

import com.cloudera.utils.hms.mirror.EnvironmentMap;
import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Conversion;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.UrlUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.*;

import static java.util.Objects.nonNull;

@Component
@Slf4j
public class DistCpService {

    private ObjectMapper yamlMapper;

    @Autowired
    public void setYamlMapper(ObjectMapper yamlMapper) {
        this.yamlMapper = yamlMapper;
    }

    public void buildAllDistCpReports(ExecuteSession session, String outputDir) {
        HmsMirrorConfig config = session.getConfig();
        Conversion conversion = session.getConversion();

        for (Map.Entry<String, DBMirror> dbEntry : conversion.getDatabases().entrySet()) {
            String database = HmsMirrorConfigUtil.getResolvedDB(dbEntry.getKey(), config);
            String originalDatabase = dbEntry.getKey();


            try {
                Environment[] environments = null;
                switch (config.getDataStrategy()) {

                    case DUMP:
                    case STORAGE_MIGRATION:
                        environments = new Environment[]{Environment.LEFT};
                        break;
                    default:
                        environments = new Environment[]{Environment.LEFT, Environment.RIGHT};
                        break;
                }

                for (Environment distcpEnv : environments) {
                    boolean dcFound = Boolean.FALSE;

                    StringBuilder distcpWorkbookSb = new StringBuilder();
                    StringBuilder distcpScriptSb = new StringBuilder();

                    distcpScriptSb.append("#!/usr/bin/env sh").append("\n");
                    distcpScriptSb.append("\n");
                    distcpScriptSb.append("# 1. Copy the source '*_distcp_source.txt' files to the distributed filesystem.").append("\n");
                    distcpScriptSb.append("# 2. Export an env var 'HCFS_BASE_DIR' that represents where these files where placed.").append("\n");
                    distcpScriptSb.append("#      NOTE: ${HCFS_BASE_DIR} must be available to the user running 'distcp'").append("\n");
                    distcpScriptSb.append("# 3. Export an env var 'DISTCP_OPTS' with any special settings needed to run the job.").append("\n");
                    distcpScriptSb.append("#      For large jobs, you may need to adjust memory settings.").append("\n");
                    distcpScriptSb.append("# 4. Run the following in an order or framework that is appropriate for your environment.").append("\n");
                    distcpScriptSb.append("#       These aren't necessarily expected to run in this shell script as is in production.").append("\n");
                    distcpScriptSb.append("\n");
                    distcpScriptSb.append("\n");
                    distcpScriptSb.append("if [ -z ${HCFS_BASE_DIR+x} ]; then").append("\n");
                    distcpScriptSb.append("  echo \"HCFS_BASE_DIR is unset\"").append("\n");
                    distcpScriptSb.append("  echo \"What is the 'HCFS_BASE_DIR':\"").append("\n");
                    distcpScriptSb.append("  read HCFS_BASE_DIR").append("\n");
                    distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
                    distcpScriptSb.append("else").append("\n");
                    distcpScriptSb.append("  echo \"HCFS_BASE_DIR is set to '$HCFS_BASE_DIR'\"").append("\n");
                    distcpScriptSb.append("fi").append("\n");
                    distcpScriptSb.append("\n");
                    distcpScriptSb.append("echo \"Creating HCFS directory: $HCFS_BASE_DIR\"").append("\n");
                    distcpScriptSb.append("hdfs dfs -mkdir -p $HCFS_BASE_DIR").append("\n");
                    distcpScriptSb.append("\n");

                    // WARNING ABOUT 'distcp' and 'table alignment'
                    distcpWorkbookSb.append("## WARNING\n");
//                            distcpWorkbookSb.append(MessageCode.RDL_DC_WARNING_TABLE_ALIGNMENT.getDesc()).append("\n\n");

                    distcpWorkbookSb.append("| Database | Target | Sources |\n");
                    distcpWorkbookSb.append("|:---|:---|:---|\n");

                    FileWriter distcpSourceFW = null;

                    Map<String, Map<String, Set<String>>> distcpPlans = buildDistcpListForDatabase(config, database, distcpEnv, 1, config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp());
                    if (!distcpPlans.isEmpty()) {
                        String distcpPlansFile = outputDir + File.separator + originalDatabase + "_" + distcpEnv.toString() + "_distcp_plans.yaml";
                        FileWriter distcpPlansFW = new FileWriter(distcpPlansFile);
                        String planYaml = yamlMapper.writeValueAsString(distcpPlans);
                        distcpPlansFW.write(planYaml);
                        distcpPlansFW.close();
                    }

                    for (Map.Entry<String, Map<String, Set<String>>> entry : distcpPlans.entrySet()) {

                        distcpWorkbookSb.append("| ").append(entry.getKey()).append(" | | |\n");
                        Map<String, Set<String>> value = entry.getValue();
                        int i = 1;
                        // https://github.com/cloudera-labs/hms-mirror/issues/105
                        // When there are multiple sources, we need to create a file for each source. BUT, when
                        // there is only one source, we can skip uses a file and just use the source directly.
                        // With 'distcp' and the '-f' option when there is only one source the last path element is
                        //   NOT carried over to the target. When there are multiple sources, the last path element
                        //   is carried over. Hence, the logic adjustment here to address the behavioral differences.
                        for (Map.Entry<String, Set<String>> dbMap : value.entrySet()) {
                            if (dbMap.getValue().size() > 1) {
                                String distcpSourceFile = entry.getKey() + "_" + distcpEnv.toString() + "_" + i++ + "_distcp_source.txt";
                                String distcpSourceFileFull = outputDir + File.separator + distcpSourceFile;
                                distcpSourceFW = new FileWriter(distcpSourceFileFull);

                                StringBuilder line = new StringBuilder();
                                line.append("| | ").append(dbMap.getKey()).append(" | ");

                                for (String source : dbMap.getValue()) {
                                    line.append(source).append("<br>");
                                    distcpSourceFW.append(source).append("\n");
                                }
                                line.append(" | ").append("\n");
                                distcpWorkbookSb.append(line);

                                distcpScriptSb.append("\n");
                                distcpScriptSb.append("echo \"Copying 'distcp' source file to $HCFS_BASE_DIR\"").append("\n");
                                distcpScriptSb.append("\n");
                                distcpScriptSb.append("hdfs dfs -copyFromLocal -f ").append(distcpSourceFile).append(" ${HCFS_BASE_DIR}").append("\n");
                                distcpScriptSb.append("\n");
                                distcpScriptSb.append("echo \"Running 'distcp'\"").append("\n");
                                // Adding -skipcrccheck to avoid failures with distcp between different protocols.
                                distcpScriptSb.append("hadoop distcp ${DISTCP_OPTS} -skipcrccheck -f ${HCFS_BASE_DIR}/").append(distcpSourceFile).append(" ").append(dbMap.getKey()).append("\n").append("\n");

                                distcpSourceFW.close();
                            } else {
                                // Only 1 entry, so we can skip the file and just use the source directly.
                                String source = dbMap.getValue().iterator().next();
                                // Get last path element
                                String lastPathElement = UrlUtils.getLastDirFromUrl(source);//).substring(source.lastIndexOf("/") + 1);

                                distcpScriptSb.append("echo \"Only one element in path.\"").append("\n");

                                String target = dbMap.getKey();

                                if (config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp()) {
                                    // Reduce the target by 1 level
                                    target = UrlUtils.reduceUrlBy(target, 1);
                                    // Concatenate the last path element to the target
//                                    if (!target.endsWith("/") && !lastPathElement.startsWith("/")) {
//                                        target += "/" + lastPathElement;
//                                    } else if (target.endsWith("/") && lastPathElement.startsWith("/")) {
//                                        target += lastPathElement.substring(1);
//                                    } else {
//                                        target += lastPathElement;
//                                    }
                                }

                                StringBuilder line = new StringBuilder();
                                line.append("| | ").append(target).append(" | ");

                                line.append(source).append(" |\n");

                                distcpWorkbookSb.append(line);

                                distcpScriptSb.append("echo \"Running 'distcp'\"").append("\n");

                                String sourceProtocol = NamespaceUtils.getProtocol(source);
                                String targetProtocol = NamespaceUtils.getProtocol(target);

                                if (nonNull(sourceProtocol) && nonNull(targetProtocol) && !sourceProtocol.equals(targetProtocol)) {
                                        distcpScriptSb.append("#  Source and target protocols are different. This may cause issues with 'distcp' is -skipcrccheck isn't set.");
                                        // Add -skipcrccheck to the distcp command
                                        distcpScriptSb.append("hadoop distcp ${DISTCP_OPTS} -skipcrccheck ").append(source).append(" ").append(target).append("\n").append("\n");
                                } else {
                                    distcpScriptSb.append("hadoop distcp ${DISTCP_OPTS} ").append(source).append(" ").append(target).append("\n").append("\n");
                                }
                            }

                            dcFound = Boolean.TRUE;
                        }
                    }

                    if (dcFound) {
                        // Set flags for report and workplan
                        // TODO: Need to re-introduce.
//                        switch (distcpEnv) {
//                            case LEFT:
//                                dcLeft = Boolean.TRUE;
//                                break;
//                            case RIGHT:
//                                dcRight = Boolean.TRUE;
//                                break;
//                        }

                        String distcpWorkbookFile = outputDir + File.separator + database +
                                "_" + distcpEnv + "_distcp_workbook.md";
                        String distcpScriptFile = outputDir + File.separator + database +
                                "_" + distcpEnv + "_distcp_script.sh";

                        FileWriter distcpWorkbookFW = new FileWriter(distcpWorkbookFile);
                        FileWriter distcpScriptFW = new FileWriter(distcpScriptFile);

                        distcpScriptFW.write(distcpScriptSb.toString());
                        distcpWorkbookFW.write(distcpWorkbookSb.toString());

                        distcpScriptFW.close();
                        distcpWorkbookFW.close();
                    }
                }
            } catch (IOException ioe) {
                log.error("Issue writing distcp workbook", ioe);
            }
        }

    }

    public synchronized Map<String, Map<String, Set<String>>> buildDistcpListForDatabase(HmsMirrorConfig config, String database,
                                                                              Environment environment, int consolidationLevel, boolean consolidateTablesForDistcp) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<>();

//        HmsMirrorConfig config = executeSessionService.getSession().getConfig();

        // get the map for a db.
//        Set<String> databases = config.getTranslator().getTranslationMap().keySet();

        // get the map.entry
        Map<String, Set<String>> reverseMap = new TreeMap<>();
        // Get a static view of set to avoid concurrent modification.
        Set<EnvironmentMap.TranslationLevel> dbTranslationLevel =
                new HashSet<>(config.getTranslator().getTranslationMap(database, environment));

        Map<String, String> dbLocationMap = new TreeMap<>();

        for (EnvironmentMap.TranslationLevel translationLevel : dbTranslationLevel) {
            if (translationLevel.getOriginal() != null &&
                    translationLevel.getTarget() != null) {
                dbLocationMap.put(translationLevel.getAdjustedOriginal(), translationLevel.getAdjustedTarget());
            }
        }

        for (Map.Entry<String, String> entry : dbLocationMap.entrySet()) {
            // reduce folder level by 'consolidationLevel' for key and value.
            // Source
//            String reducedSource = UrlUtils.reduceUrlBy(entry.getKey(), consolidationLevel);
            String reducedSource = entry.getKey();
            // Target
//            String reducedTarget = UrlUtils.reduceUrlBy(entry.getValue(), consolidationLevel);
            String reducedTarget = entry.getValue();

            if (reverseMap.get(reducedTarget) != null) {
                reverseMap.get(reducedTarget).add(entry.getKey());
            } else {
                Set<String> sourceSet = new TreeSet<String>();
                sourceSet.add(entry.getKey());
                reverseMap.put(reducedTarget, sourceSet);
            }

        }
        if (!reverseMap.isEmpty()) {
            rtn.put(database, reverseMap);
        }
        return rtn;
    }

}
