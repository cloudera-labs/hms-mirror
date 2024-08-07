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

package com.cloudera.utils.hms.mirror.datastrategy;

//import com.cloudera.utils.hadoop.HadoopSession;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.util.NamespaceUtils;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Slf4j
@Getter
public abstract class DataStrategyBase implements DataStrategy {

    public static final Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static final Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    protected ExecuteSessionService executeSessionService;

    protected Boolean AVROCheck(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        Boolean relative = Boolean.FALSE;
        HmsMirrorConfig hmsMirrorConfig = executeSessionService.getSession().getConfig();

        // Check for AVRO
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        if (TableUtils.isAVROSchemaBased(let)) {
            log.info("{}: is an AVRO table.", let.getName());
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            log.debug("{}: Original AVRO Schema path: {}", let.getName(), leftPath);
                /* Checks:
                - Is Path prefixed with a protocol?
                    - (Y) Does it match the LEFT's hcfsNamespace.
                        - (Y) Replace prefix with RIGHT 'hcfsNamespace' prefix.
                        - (N) Throw WARNING and set return to FALSE.  We don't recognize the prefix and
                                 can't guarantee that we can retrieve the file.
                    - (N) Leave it and copy the file to the same relative path on the RIGHT
                 */
            String leftNamespace = NamespaceUtils.getNamespace(leftPath);
            try {
                if (nonNull(leftNamespace)) {
                    log.info("{}: Namespace found: {}", let.getName(), leftNamespace);
                    rightPath = NamespaceUtils.replaceNamespace(leftPath, hmsMirrorConfig.getTargetNamespace());
//            } else {
//                log.info("{}: Namespace NOT found.", let.getName());
//            }

//            Matcher matcher = protocolNSPattern.matcher(leftPath);
//            // ProtocolNS Found.
//            String cpCmd = null;
//            if (matcher.find()) {
//                log.info("{} protocol Matcher found.", let.getName());
//
//                // Return the whole set of groups.
//                String lns = matcher.group(0);
//
//                // Does it match the "LEFT" hcfsNamespace.
//                String leftNS = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace();
//                if (leftNS.endsWith("/")) {
//                    leftNS = leftNS.substring(0, leftNS.length() - 1);
//                }
//                if (lns.startsWith(leftNS)) {
//                    log.info("{} table namespace matches LEFT clusters namespace.", let.getName());
//
//                    // They match, so replace with RIGHT hcfs namespace.
//                    String newNS = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace();
//                    if (newNS.endsWith("/")) {
//                        newNS = newNS.substring(0, newNS.length() - 1);
//                    }
//                    rightPath = leftPath.replace(leftNS, newNS);
//                    log.info("{} table namespace adjusted for RIGHT clusters table to {}", ret.getName(), rightPath);
//                    TableUtils.updateAVROSchemaLocation(ret, rightPath);
//                } else {
//                    // Protocol found doesn't match configured hcfs namespace for LEFT.
//                    String warning = "AVRO Schema URL was NOT adjusted. Current (LEFT) path did NOT match the " +
//                            "LEFT hcfsnamespace. " + leftPath + " is NOT in the " + hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace() +
//                            ". Can't determine change, so we'll not do anything.";
//                    ret.addIssue(warning);
//                    ret.addIssue("Schema creation may fail if location isn't available to RIGHT cluster.");
//                    log.warn(warning);
//                }
                } else {
                    // No Protocol defined.  So we're assuming that its a relative path to the
                    // defaultFS
                    String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
                    log.info("{}: {}", let.getName(), rpath);
                    ret.addIssue(rpath);
                    rightPath = leftPath;
                    relative = Boolean.TRUE;
                }

                if (nonNull(leftPath) && nonNull(rightPath) && hmsMirrorConfig.isCopyAvroSchemaUrls() && hmsMirrorConfig.isExecute()) {
                    // Copy over.
                    log.info("{}: Attempting to copy AVRO schema file to target cluster.", let.getName());
                    CliEnvironment cli = executeSessionService.getCliEnvironment();
                    try {
                        CommandReturn cr = null;
                        if (relative) {
                            // checked..
                            // TODO: We won't have the left namespace.
//                            leftPath = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace() + leftPath;
                            rightPath = hmsMirrorConfig.getTargetNamespace() + rightPath;
                        }
                        log.info("AVRO Schema COPY from: {} to {}", leftPath, rightPath);
                        // Ensure the path for the right exists.
                        String parentDirectory = NamespaceUtils.getParentDirectory(rightPath);
                        if (nonNull(parentDirectory)) {
//                        String pathEnd = matcher.group(1);
//                        String mkdir = rightPath.substring(0, rightPath.length() - pathEnd.length());
                            cr = cli.processInput("mkdir -p " + parentDirectory);
                            if (cr.isError()) {
                                ret.addIssue("Problem creating directory " + parentDirectory + ". " + cr.getError());
                                rtn = Boolean.FALSE;
                            } else {
                                cr = cli.processInput("cp -f " + leftPath + " " + rightPath);
                                if (cr.isError()) {
                                    ret.addIssue("Problem copying AVRO schema file from " + leftPath + " to " +
                                            parentDirectory + ".\n```" + cr.getError() + "```");
                                    rtn = Boolean.FALSE;
                                }
                            }
                        }
                    } catch (Throwable t) {
                        log.error("{}: AVRO file copy issue", ret.getName(), t);
                        ret.addIssue(t.getMessage());
                        rtn = Boolean.FALSE;
//                } finally {
//                    if (session != null)
//                        config.getCliEnv().returnSession(session);
                    }
                } else {
                    log.info("{}: did NOT attempt to copy AVRO schema file to target cluster.", let.getName());
                }
                tableMirror.addStep("AVRO", "Checked");
            } catch (RequiredConfigurationException e) {
                log.error("Required Configuration Exception", e);
                ret.addIssue(e.getMessage());
                rtn = Boolean.FALSE;
            }
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public EnvironmentTable getEnvironmentTable(Environment environment, TableMirror tableMirror) {
        EnvironmentTable et = tableMirror.getEnvironments().get(environment);
        if (isNull(et)) {
            et = new EnvironmentTable(tableMirror);
            tableMirror.getEnvironments().put(environment, et);
        }
        return et;
    }

}
