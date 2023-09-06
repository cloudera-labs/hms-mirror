package com.cloudera.utils.hadoop.hms.mirror.datastrategy;

import com.cloudera.utils.hadoop.HadoopSession;
import com.cloudera.utils.hadoop.hms.mirror.*;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class DataStrategyBase implements DataStrategy {
    private static final Logger LOG = LogManager.getLogger(DataStrategyBase.class);
    public static Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");

    protected Config config;
    protected DBMirror dbMirror;
    protected TableMirror tableMirror;

    @Override
    public void setDBMirror(DBMirror dbMirror) {
        this.dbMirror = dbMirror;
    }

    @Override
    public DBMirror getDBMirror() {
        return dbMirror;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public void setTableMirror(TableMirror tableMirror) {
        this.tableMirror = tableMirror;
    }

    @Override
    public TableMirror getTableMirror() {
        return tableMirror;
    }

    public EnvironmentTable getEnvironmentTable(Environment environment) {
        EnvironmentTable et = getTableMirror().getEnvironments().get(environment);
        if (et == null) {
            et = new EnvironmentTable(getTableMirror());
            getTableMirror().getEnvironments().put(environment, et);
        }
        return et;
    }

    protected Boolean AVROCheck() {
        Boolean rtn = Boolean.TRUE;
        Boolean relative = Boolean.FALSE;
        // Check for AVRO
        EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
        EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);
        if (TableUtils.isAVROSchemaBased(let)) {
            LOG.info(let.getName() + ": is an AVRO table.");
            String leftPath = TableUtils.getAVROSchemaPath(let);
            String rightPath = null;
            LOG.debug(let.getName() + ": Original AVRO Schema path: " + leftPath);
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
                LOG.info(let.getName() + " protocol Matcher found.");

                // Return the whole set of groups.
                String lns = matcher.group(0);

                // Does it match the "LEFT" hcfsNamespace.
                String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
                if (leftNS.endsWith("/")) {
                    leftNS = leftNS.substring(0, leftNS.length() - 1);
                }
                if (lns.startsWith(leftNS)) {
                    LOG.info(let.getName() + " table namespace matches LEFT clusters namespace.");

                    // They match, so replace with RIGHT hcfs namespace.
                    String newNS = config.getCluster(Environment.RIGHT).getHcfsNamespace();
                    if (newNS.endsWith("/")) {
                        newNS = newNS.substring(0, newNS.length() - 1);
                    }
                    rightPath = leftPath.replace(leftNS, newNS);
                    LOG.info(ret.getName() + " table namespace adjusted for RIGHT clusters table to " + rightPath);
                    TableUtils.updateAVROSchemaLocation(ret, rightPath);
                } else {
                    // Protocol found doesn't match configured hcfs namespace for LEFT.
                    String warning = "AVRO Schema URL was NOT adjusted. Current (LEFT) path did NOT match the " +
                            "LEFT hcfsnamespace. " + leftPath + " is NOT in the " + config.getCluster(Environment.LEFT).getHcfsNamespace() +
                            ". Can't determine change, so we'll not do anything.";
                    ret.addIssue(warning);
                    ret.addIssue("Schema creation may fail if location isn't available to RIGHT cluster.");
                    LOG.warn(warning);
                }
            } else {
                // No Protocol defined.  So we're assuming that its a relative path to the
                // defaultFS
                String rpath = "AVRO Schema URL appears to be relative: " + leftPath + ". No table definition adjustments.";
                LOG.info(let.getName() + ": " + rpath);
                ret.addIssue(rpath);
                rightPath = leftPath;
                relative = Boolean.TRUE;
            }

            if (leftPath != null && rightPath != null && config.isCopyAvroSchemaUrls() && config.isExecute()) {
                // Copy over.
                LOG.info(let.getName() + ": Attempting to copy AVRO schema file to target cluster.");
                HadoopSession session = null;
                try {
                    session = config.getCliPool().borrow();
                    CommandReturn cr = null;
                    if (relative) {
                        leftPath = config.getCluster(Environment.LEFT).getHcfsNamespace() + leftPath;
                        rightPath = config.getCluster(Environment.RIGHT).getHcfsNamespace() + rightPath;
                    }
                    LOG.info("AVRO Schema COPY from: " + leftPath + " to " + rightPath);
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
                    LOG.error(ret.getName() + ": AVRO file copy issue", t);
                    ret.addIssue(t.getMessage());
                    rtn = Boolean.FALSE;
                } finally {
                    if (session != null)
                        config.getCliPool().returnSession(session);
                }
            } else {
                LOG.info(let.getName() + ": did NOT attempt to copy AVRO schema file to target cluster.");
            }
            tableMirror.addStep("AVRO", "Checked");
        } else {
            // Not AVRO, so no action (passthrough)
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

}
