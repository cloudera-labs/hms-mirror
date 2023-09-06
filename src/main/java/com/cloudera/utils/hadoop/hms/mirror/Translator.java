/*
 * Copyright 2021 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.utils.hadoop.hms.mirror;

import com.cloudera.utils.hadoop.hms.Context;
import com.cloudera.utils.hadoop.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hadoop.hms.stage.Transfer;
import com.cloudera.utils.hadoop.hms.util.TableUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;

import static com.cloudera.utils.hadoop.hms.mirror.MessageCode.*;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.*;
import static com.cloudera.utils.hadoop.hms.mirror.MirrorConf.DB_MANAGED_LOCATION;

@JsonIgnoreProperties({"dbLocationMap"})
public class Translator {
    private static final Logger LOG = LogManager.getLogger(Translator.class);

    /*
    Use this to force the location element in the external table create statements and
    not rely on the database 'location' element.
     */
    private Boolean forceExternalLocation = Boolean.FALSE;

    @JsonIgnore
    private final Map<String, EnvironmentMap> dbLocationMap = new TreeMap<String, EnvironmentMap>();

    private Map<String, String> globalLocationMap = null;

    public Boolean getForceExternalLocation() {
        return forceExternalLocation;
    }

    public void setForceExternalLocation(Boolean forceExternalLocation) {
        this.forceExternalLocation = forceExternalLocation;
    }

    public Map<String, String> getGlobalLocationMap() {
        if (globalLocationMap == null) {
            Comparator<String> stringLengthComparator = new Comparator<String>() {
                // return comparison of two strings, first by length then by value.
                public int compare(String k1, String k2) {
                    int comp = 0;
                    if (k1.length() > k2.length()) {
                        comp = -1;
                    } else if (k1.length() == k2.length()) {
                        comp = 0;
                    } else {
                        comp = 1;
                    }
                    if (comp == 0)
                        comp = k1.compareTo(k2);
                    return comp;
                }
            };
            globalLocationMap = new TreeMap<String, String>(stringLengthComparator);
        }
        return globalLocationMap;
    }

    public void setGlobalLocationMap(Map<String, String> globalLocationMap) {
        getGlobalLocationMap().putAll(globalLocationMap);
    }

    public void addGlobalLocationMap(String from, String to) {
        getGlobalLocationMap().put(from, to);
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;
        return rtn;
    }

    public static String removeLastDirFromUrl(final String url) {
        Matcher matcher = Transfer.lastDirPattern.matcher(url);
        if (matcher.find()) {
            String matchStr = matcher.group(1);
            // Remove last occurrence ONLY.
            Integer lastIndexOf = url.lastIndexOf(matchStr);
            return url.substring(0, lastIndexOf - 1);
        } else {
            return url;
        }
    }

    public static String replaceLast(String text, String regex, String replacement) {
        return text.replaceFirst("(?s)" + regex + "(?!.*?" + regex + ")", replacement);
    }

    public static String reduceUrlBy(String url, int level) {
        String rtn = url.trim();
        if (rtn.endsWith("/"))
            rtn = rtn.substring(0, rtn.length() - 2);
        for (int i = 0; i < level; i++) {
            rtn = removeLastDirFromUrl(rtn);
        }
        return rtn;
    }

    public String processGlobalLocationMap(String originalLocation) {
        String newLocation = null;
        if (getGlobalLocationMap().size() > 0) {
            LOG.debug("Checking location: " + originalLocation + " for replacement element in " +
                    "global location map.");
            for (String key : getGlobalLocationMap().keySet()) {
                if (originalLocation.startsWith(key)) {
                    String rLoc = getGlobalLocationMap().get(key);
                    newLocation = originalLocation.replace(key, rLoc);
                    LOG.info("Location Map Found. " + key +
                            ":" + rLoc + " New Location: " + newLocation);
                    // Stop Processing
                    break;
                }
            }
        }
        if (newLocation != null)
            return newLocation;
        else
            return originalLocation;
    }

    public String buildPartitionAddStatement(EnvironmentTable environmentTable) {
        StringBuilder sbPartitionDetails = new StringBuilder();
        Map<String, String> partitions = new HashMap<String, String>();
        // Fix formatting of partition names.
        for (Map.Entry<String, String> item : environmentTable.getPartitions().entrySet()) {
            String partitionName = item.getKey();
            String partSpec = TableUtils.toPartitionSpec(partitionName);
            partitions.put(partSpec, item.getValue());
        }
        // Transfer partitions map to a string using streaming
        partitions.entrySet().stream().forEach(e -> sbPartitionDetails.append("\tPARTITION (" + e.getKey() + ") LOCATION '" + e.getValue() + "' \n"));
        return sbPartitionDetails.toString();
    }

    public Boolean translatePartitionLocations(TableMirror tblMirror) {
        Boolean rtn = Boolean.TRUE;
        Config config = Context.getInstance().getConfig();
        Map<String, String> dbRef = tblMirror.getParent().getDBDefinition(Environment.RIGHT);
        Boolean chkLocation = config.getTransfer().getWarehouse().getManagedDirectory() != null && config.getTransfer().getWarehouse().getExternalDirectory() != null;
        if (config.getEvaluatePartitionLocation() && tblMirror.getEnvironmentTable(Environment.LEFT).getPartitioned()
                && (tblMirror.getStrategy() == DataStrategyEnum.SCHEMA_ONLY)) {
            // Only Translate for SCHEMA_ONLY.  Leave the DUMP location as is.
            EnvironmentTable target = tblMirror.getEnvironmentTable(Environment.RIGHT);
            /*
            Review the target partition locations and replace the namespace with the new namespace.
            Check whether any global location maps match the location and adjust.
             */
            Map<String, String> partitionLocationMap = target.getPartitions();
            if (partitionLocationMap != null && !partitionLocationMap.isEmpty()) {
                for (Map.Entry<String, String> entry : partitionLocationMap.entrySet()) {
                    String partitionLocation = entry.getValue();
                    String partSpec = entry.getKey();
                    int level = StringUtils.countMatches(partSpec, "/");
                    // Increase level to the table, since we're not filter any tables.  It's assumed that
                    //   we're pulling the whole DB.
                    if (!config.getFilter().isTableFiltering()) {
                        level++;
                    }
                    if (partitionLocation == null || partitionLocation.isEmpty() ||
                            partitionLocation.equals(NOT_SET)) {
                        rtn = Boolean.FALSE;
                        continue;
                    }
                    // Get the relative dir.
                    String relativeDir = partitionLocation.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(), "");
                    // Check the Global Location Map for a match.
                    String mappedDir = processGlobalLocationMap(relativeDir);
                    if (relativeDir.equals(mappedDir) && config.getResetToDefaultLocation()) {
                        // This is a problem, since we've asked to translate the partitions but didn't find a map, nothing changed.
                        // Which would be inconsistent with the table location details.
                        String errMsg = MessageFormat.format(RDL_W_EPL_NO_MAPPING.getDesc(), entry.getKey(), entry.getValue());
                        tblMirror.addIssue(Environment.RIGHT, errMsg);
                        rtn = Boolean.FALSE;
                    }
                    // Check for 'common storage'
                    String newPartitionLocation = null;
                    if (config.getTransfer().getCommonStorage() != null) {
                        newPartitionLocation = config.getTransfer().getCommonStorage() + mappedDir;
                    } else {
                        newPartitionLocation = config.getCluster(Environment.RIGHT).getHcfsNamespace() + mappedDir;
                    }
                    entry.setValue(newPartitionLocation);
                    // For distcp.
                    addLocation(tblMirror.getParent().getResolvedName(), Environment.RIGHT, partitionLocation,
                            newPartitionLocation, ++level);

                    // Check and warn against warehouse locations if specified.
                    if (config.getTransfer().getWarehouse().getExternalDirectory() != null &&
                            config.getTransfer().getWarehouse().getManagedDirectory() != null) {
                        if (TableUtils.isExternal(tblMirror.getEnvironmentTable(Environment.LEFT))) {
                            // We store the DB LOCATION in the RIGHT dbDef so we can avoid changing the original LEFT
                            if (!newPartitionLocation.startsWith(tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION))) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                        tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_LOCATION),
                                        newPartitionLocation);
                                tblMirror.addIssue(Environment.RIGHT, msg);
                            }
                        } else {
                            if (!newPartitionLocation.startsWith(tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION))) {
                                // Set warning that even though you've specified to warehouse directories, the current configuration
                                // will NOT place it in that directory.
                                String msg = MessageFormat.format(LOCATION_NOT_MATCH_WAREHOUSE.getDesc(), "partition",
                                        tblMirror.getParent().getDBDefinition(Environment.RIGHT).get(DB_MANAGED_LOCATION),
                                        newPartitionLocation);
                                tblMirror.addIssue(Environment.RIGHT, msg);
                            }
                        }
                    }

                }
            }
            // end partitions location conversion.
        }
        return rtn;
    }

    public String translateTableLocation(TableMirror tableMirror, String originalLocation, int level, String partitionSpec) throws Exception{
        String rtn = originalLocation;
        StringBuilder dirBuilder = new StringBuilder();
        String tableName = tableMirror.getName();
        String dbName = tableMirror.getParent().getResolvedName();

        Config config = Context.getInstance().getConfig();

        String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
        // Set base on rightNS or Common Storage, if specified
        String rightNS = config.getTransfer().getCommonStorage() == null ?
                config.getCluster(Environment.RIGHT).getHcfsNamespace() : config.getTransfer().getCommonStorage();

        // Get the relative dir.
        if (!rtn.startsWith(config.getCluster(Environment.LEFT).getHcfsNamespace())) {
            throw new Exception("Table/Partition Location prefix: `" + originalLocation +
                    "` doesn't match the LEFT clusters defined hcfsNamespace: `" + config.getCluster(Environment.LEFT).getHcfsNamespace() +
                    "`. We can't reliably make this translation.");
        }
        String relativeDir = rtn.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(), "");
        // Check the Global Location Map for a match.
        String mappedDir = processGlobalLocationMap(relativeDir);
        // If they don't match, it was reMapped!
        Boolean reMapped = !relativeDir.equals(mappedDir);
        if (reMapped) {
            tableMirror.setReMapped(Boolean.TRUE);
        } else {
            // under conditions like, STORAGE_MIGRATION, same namespace, !rdl and glm we need to ensure ALL locations are
            //   mapped...  If they aren't, they won't be moved as the translation wouldn't change.  So we need to throw
            //   an error that ensures the table fails to process.
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION &&
                    config.getTransfer().getCommonStorage().equals(config.getCluster(Environment.LEFT).getHcfsNamespace()) &&
                    !config.getResetToDefaultLocation()) {
                throw new RuntimeException("Location Mapping can't be determined.  No matching `glm` entry to make translation." +
                        "Original Location: " + originalLocation);
            }
        }
        // Feature Off.  Basic translation which includes any GlobalLocationMaps.
        String newLocation = null;
        StringBuilder sbDir = new StringBuilder();
        if (config.getTransfer().getCommonStorage() != null) {
            sbDir.append(config.getTransfer().getCommonStorage());
        } else {
            sbDir.append(rightNS);
        }
        if (reMapped) {
            sbDir.append(mappedDir);
            newLocation = sbDir.toString();
        } else if (config.getResetToDefaultLocation() && config.getTransfer().getWarehouse().getExternalDirectory() != null) {
            // RDL and EWD
            sbDir.append(config.getTransfer().getWarehouse().getExternalDirectory()).append("/");
            sbDir.append(dbName).append(".db").append("/").append(tableName);
            if (partitionSpec != null)
                sbDir.append("/").append(partitionSpec);
            newLocation = sbDir.toString();
        } else {
            switch (config.getDataStrategy()) {
                case EXPORT_IMPORT:
                case HYBRID:
                case SQL:
                case SCHEMA_ONLY:
                case DUMP:
                case STORAGE_MIGRATION:
                case CONVERT_LINKED:
                    newLocation = originalLocation.replace(leftNS, rightNS);
                    break;
                case LINKED:
                case COMMON:
                    newLocation = originalLocation;
                    break;
            }
        }
        dirBuilder.append(newLocation);

        LOG.debug("Translate Table Location: " + originalLocation + ": " + dirBuilder);
        // Add Location Map for table to a list.
        // TODO: Need to handle RIGHT locations.
        if (config.getTransfer().getStorageMigration().isDistcp() && config.getDataStrategy() != DataStrategyEnum.SQL) {
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
                addLocation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            } else if (config.getTransfer().getStorageMigration().getDataFlow() == DistcpFlow.PULL && !config.isFlip()) {
                addLocation(dbName, Environment.RIGHT, originalLocation, dirBuilder.toString().trim(), level);
            } else {
                addLocation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            }
        }

        return dirBuilder.toString().trim();
    }

    public void addLocation(String database, Environment environment, String originalLocation, String newLocation, int level) {
        EnvironmentMap environmentMap = dbLocationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        environmentMap.addTranslationLocation(environment, originalLocation, newLocation, level);
//        getDbLocationMap(database, environment).put(originalLocation, newLocation);
    }

    private synchronized Set<EnvironmentMap.TranslationLevel> getDbLocationMap(String database, Environment environment) {
        EnvironmentMap envMap = dbLocationMap.computeIfAbsent(database, k -> new EnvironmentMap());
        return envMap.getTranslationSet(environment);
    }

    /**
     * @param consolidationLevel how far up the directory hierarchy to go to build the distcp list based on the sources
     *                           provided.
     * @return A map of databases.  Each database will have a map that has 1 or more 'targets' and 'x' sources for each
     * target.
     */
    public Map<String, Map<String, Set<String>>> buildDistcpList(String database, Environment environment, int consolidationLevel) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<>();
        // dbLocationMap Map<String, Map<String, String>>

        // get the map for a db.
        Set<String> databases = dbLocationMap.keySet();

//        for (String database: databases) {
        // get the map.entry
        Map<String, Set<String>> reverseMap = new TreeMap<String, Set<String>>();
        Set<EnvironmentMap.TranslationLevel> dbTranslationLevel = getDbLocationMap(database, environment);

        Map<String, String> dbLocationMap = new TreeMap<>();

        for (EnvironmentMap.TranslationLevel translationLevel : dbTranslationLevel) {
            dbLocationMap.put(translationLevel.getAdjustedOriginal(), translationLevel.getAdjustedTarget());
        }

        for (Map.Entry<String, String> entry : dbLocationMap.entrySet()) {
            // reduce folder level by 'consolidationLevel' for key and value.
            // Source
            String reducedSource = Translator.reduceUrlBy(entry.getKey(), consolidationLevel);
            // Target
            String reducedTarget = Translator.reduceUrlBy(entry.getValue(), consolidationLevel);

            if (reverseMap.get(reducedTarget) != null) {
                reverseMap.get(reducedTarget).add(entry.getKey());
            } else {
                Set<String> sourceSet = new TreeSet<String>();
                sourceSet.add(entry.getKey());
                reverseMap.put(reducedTarget, sourceSet);
            }

        }
        rtn.put(database, reverseMap);
//        }
        return rtn;
    }

}
