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
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static java.util.Objects.nonNull;

/**
 * Service responsible for generating DistCp (distributed copy) reports and scripts
 * for Hive Metastore mirroring operations.
 * 
 * <p>This service creates detailed reports and executable scripts that facilitate
 * data migration between different environments using Hadoop's DistCp utility.</p>
 * 
 * <p>The service generates:
 * <ul>
 *   <li>DistCp workbooks (markdown files) documenting the migration plan</li>
 *   <li>DistCp scripts (shell scripts) that can be executed to perform the migration</li>
 *   <li>DistCp source files containing the list of paths to be copied</li>
 * </ul>
 * </p>
 */
@Component
@Slf4j
public class DistCpService {

    private final ObjectMapper yamlMapper;

    /**
     * Constructor for DistCpService.
     *
     * @param yamlMapper The ObjectMapper configured for YAML serialization
     */
    public DistCpService(ObjectMapper yamlMapper) {
        this.yamlMapper = yamlMapper;
    }

    /**
     * Builds DistCp reports and scripts for all databases in the conversion.
     *
     * <p>This method generates:
     * <ul>
     *   <li>DistCp workbooks (markdown files) documenting the migration plan</li>
     *   <li>DistCp scripts (shell scripts) that can be executed to perform the migration</li>
     *   <li>DistCp source files containing the list of paths to be copied</li>
     * </ul>
     * </p>
     *
     * @param session The execution session containing configuration and conversion data
     * @param outputDir The directory where the generated files will be written
     */
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

                    Map<String, Map<String, Set<String>>> distcpPlans = buildDistcpListForDatabase(
                            config, originalDatabase, distcpEnv, 1, 
                            config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp());
                    
                    if (!distcpPlans.isEmpty()) {
                        String distcpPlansFile = outputDir + File.separator + originalDatabase + "_" + 
                                distcpEnv.toString() + "_distcp_plans.yaml";
                        FileWriter distcpPlansFW = new FileWriter(distcpPlansFile);
                        String planYaml = yamlMapper.writeValueAsString(distcpPlans);
                        distcpPlansFW.write(planYaml);
                        distcpPlansFW.close();
                    }

                    for (Map.Entry<String, Map<String, Set<String>>> entry : distcpPlans.entrySet()) {
                        distcpWorkbookSb.append("| ").append(entry.getKey()).append(" | | |\n");
                        Map<String, Set<String>> value = entry.getValue();
                        int i = 1;
                        
                        for (Map.Entry<String, Set<String>> dbMap : value.entrySet()) {
                            if (dbMap.getValue().size() > 1) {
                                String distcpSourceFile = entry.getKey() + "_" + distcpEnv + "_" + i++ + "_distcp_source.txt";
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
                                String lastPathElement = UrlUtils.getLastDirFromUrl(source);

                                distcpScriptSb.append("echo \"Only one element in path.\"").append("\n");

                                String target = dbMap.getKey();

                                if (config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp()) {
                                    // Reduce the target by 1 level
                                    target = UrlUtils.reduceUrlBy(target, 1);
                                }

                                String line = "| | " + target + " | " + source + " |\n";
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
                        String distcpWorkbookFile = outputDir + File.separator + originalDatabase +
                                "_" + distcpEnv + "_distcp_workbook.md";
                        String distcpScriptFile = outputDir + File.separator + originalDatabase +
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

    /**
     * Builds a list of DistCp operations for a database.
     *
     * <p>This method creates a map of target locations to sets of source locations
     * that need to be copied using DistCp.</p>
     *
     * @param config The HmsMirrorConfig
     * @param database The database name
     * @param environment The environment (LEFT or RIGHT)
     * @param consolidationLevel The level at which to consolidate paths
     * @param consolidateTablesForDistcp Whether to consolidate tables for DistCp
     * @return A map of database names to maps of target locations to sets of source locations
     */
    public synchronized Map<String, Map<String, Set<String>>> buildDistcpListForDatabase(
            HmsMirrorConfig config, String database, Environment environment, 
            int consolidationLevel, boolean consolidateTablesForDistcp) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<>();

        // Get a static view of set to avoid concurrent modification.
        Set<EnvironmentMap.TranslationLevel> dbTranslationLevel =
                new HashSet<>(config.getTranslator().getTranslationMap(database, environment));

        Map<String, String> dbLocationMap = new TreeMap<>();

        // Build a map of original locations to target locations
        for (EnvironmentMap.TranslationLevel translationLevel : dbTranslationLevel) {
            if (translationLevel.getOriginal() != null && translationLevel.getTarget() != null) {
                dbLocationMap.put(translationLevel.getAdjustedOriginal(), translationLevel.getAdjustedTarget());
            }
        }

        // Build a reverse map of target locations to sets of source locations
        Map<String, Set<String>> reverseMap = new TreeMap<>();
        for (Map.Entry<String, String> entry : dbLocationMap.entrySet()) {
            String reducedSource = entry.getKey();
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
