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

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hms.mirror.Config;
import com.cloudera.utils.hms.mirror.Environment;
import com.cloudera.utils.hms.mirror.EnvironmentTable;
import com.cloudera.utils.hms.mirror.TableMirror;
import com.cloudera.utils.hms.mirror.service.ConfigService;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public abstract class DataStrategyBase implements DataStrategy {

    public static Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    @Getter
    protected ConfigService configService;

    protected Boolean AVROCheck(TableMirror tableMirror) {
        Boolean rtn = Boolean.TRUE;
        Boolean relative = Boolean.FALSE;
        Config config = getConfigService().getConfig();

        // Check for AVRO
        EnvironmentTable let = getEnvironmentTable(Environment.LEFT, tableMirror);
        EnvironmentTable ret = getEnvironmentTable(Environment.RIGHT, tableMirror);
        if (TableUtils.isAVROSchemaBased(let)) {
            log.info(let.getName() + ": is an AVRO table.");
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            log.debug(let.getName() + ": Original AVRO Schema path: " + leftPath);
                /* Checks:
                - Is Path prefixed with a protocol?
                    - (Y) Does it match the LEFT's hcfsNamespace.
                        - (Y) Replace prefix with RIGHT 'hcfsNamespace' prefix.
                        - (N) Throw WARNING and set return to FALSE.  We don't recognize the prefix and
                                 can't guarantee that we can retrieve the file.
                    - (N) Leave it and copy the file to the same relative path on the RIGHT
                 */
            Matcher matcher = protocolNSPattern.matcher(leftPath);
            // ProtocolNS Found.
            String cpCmd = null;
            if (matcher.find()) {
                log.info(let.getName() + " protocol Matcher found.");

                // Return the whole set of groups.
                String lns = matcher.group(0);

                // Does it match the "LEFT" hcfsNamespace.
                String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
                if (leftNS.endsWith("/")) {
                    leftNS = leftNS.substring(0, leftNS.length() - 1);
                }
                if (lns.startsWith(leftNS)) {
                    log.info(let.getName() + " table namespace matches LEFT clusters namespace.");

                    // They match, so replace with RIGHT hcfs namespace.
                    String newNS = config.getCluster(Environment.RIGHT).getHcfsNamespace();
                    if (newNS.endsWith("/")) {
                        newNS = newNS.substring(0, newNS.length() - 1);
                    }
                    rightPath = leftPath.replace(leftNS, newNS);
                    log.info(ret.getName() + " table namespace adjusted for RIGHT clusters table to " + rightPath);
                    TableUtils.updateAVROSchemaLocation(ret, rightPath);
                } else {
                    // Protocol found doesn't match configured hcfs namespace for LEFT.
                    String warning = "AVRO Schema URL was NOT adjusted. Current (LEFT) path did NOT match the " +
                            "LEFT hcfsnamespace. " + leftPath + " is NOT in the " + config.getCluster(Environment.LEFT).getHcfsNamespace() +
                            ". Can't determine change, so we'll not do anything.";
                    ret.addIssue(warning);
                    ret.addIssue("Schema creation may fail if location isn't available to RIGHT cluster.");
                    log.warn(warning);
                }
            } else {
                // No Protocol defined.  So we're assuming that its a relative path to the
                // defaultFS
                String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
                log.info(let.getName() + ": " + rpath);
                ret.addIssue(rpath);
                rightPath = leftPath;
                relative = Boolean.TRUE;
            }

            if (leftPath != null && rightPath != null && config.isCopyAvroSchemaUrls() && config.isExecute()) {
                // Copy over.
                log.info(let.getName() + ": Attempting to copy AVRO schema file to target cluster.");
                HadoopSession session = null;
                try {
                    session = config.getCliPool().borrow();
                    CommandReturn cr = null;
                    if (relative) {
                        leftPath = config.getCluster(Environment.LEFT).getHcfsNamespace() + leftPath;
                        rightPath = config.getCluster(Environment.RIGHT).getHcfsNamespace() + rightPath;
                    }
                    log.info("AVRO Schema COPY from: " + leftPath + " to " + rightPath);
                    // Ensure the path for the right exists.
                    matcher = lastDirPattern.matcher(rightPath);
                    if (matcher.find()) {
                        String pathEnd = matcher.group(1);
                        String mkdir = rightPath.substring(0, rightPath.length() - pathEnd.length());
                        cr = session.processInput("mkdir -p " + mkdir);
                        if (cr.isError()) {
                            ret.addIssue("Problem creating directory " + mkdir + ". " + cr.getError());
                            rtn = Boolean.FALSE;
                        } else {
                            cr = session.processInput("cp -f " + leftPath + " " + rightPath);
                            if (cr.isError()) {
                                ret.addIssue("Problem copying AVRO schema file from " + leftPath + " to " +
                                        mkdir + ".\n```" + cr.getError() + "```");
                                rtn = Boolean.FALSE;
                            }
                        }
                    }
                } catch (Throwable t) {
                    log.error(ret.getName() + ": AVRO file copy issue", t);
                    ret.addIssue(t.getMessage());
                    rtn = Boolean.FALSE;
                } finally {
                    if (session != null)
                        config.getCliPool().returnSession(session);
                }
            } else {
                log.info(let.getName() + ": did NOT attempt to copy AVRO schema file to target cluster.");
            }
            tableMirror.addStep("AVRO", "Checked");
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    public EnvironmentTable getEnvironmentTable(Environment environment, TableMirror tableMirror) {
        EnvironmentTable et = tableMirror.getEnvironments().get(environment);
        if (et == null) {
            et = new EnvironmentTable(tableMirror);
            tableMirror.getEnvironments().put(environment, et);
        }
        return et;
    }

}
