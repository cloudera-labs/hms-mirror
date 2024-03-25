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

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.*;

import static com.cloudera.utils.hms.mirror.MessageCode.LOCATION_NOT_MATCH_WAREHOUSE;
import static com.cloudera.utils.hms.mirror.MessageCode.RDL_W_EPL_NO_MAPPING;
import static com.cloudera.utils.hms.mirror.MirrorConf.*;

@Service
@Slf4j
@Getter
@Setter
public class TranslatorService {
    @Getter
    private ConfigService configService = null;

    /**
     * @param consolidationLevel how far up the directory hierarchy to go to build the distcp list based on the sources
     *                           provided.
     * @return A map of databases.  Each database will have a map that has 1 or more 'targets' and 'x' sources for each
     * target.
     */
    public synchronized Map<String, Map<String, Set<String>>> buildDistcpList(String database, Environment environment, int consolidationLevel) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<>();

        // get the map for a db.
        Set<String> databases = getConfigService().getConfig().getTranslator().getDbLocationMap().keySet();

        // get the map.entry
        Map<String, Set<String>> reverseMap = new TreeMap<String, Set<String>>();
        // Get a static view of set to avoid concurrent modification.
        Set<EnvironmentMap.TranslationLevel> dbTranslationLevel = new HashSet<>();
        dbTranslationLevel.addAll(getConfigService().getConfig().getTranslator().getDbLocationMap(database, environment));

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
        return rtn;
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

    public String processGlobalLocationMap(String originalLocation) {
        String newLocation = null;
        if (!getConfigService().getConfig().getTranslator().getGlobalLocationMap().isEmpty()) {
            log.debug("Checking location: " + originalLocation + " for replacement element in " +
                    "global location map.");
            for (String key : getConfigService().getConfig().getTranslator().getGlobalLocationMap().keySet()) {
                if (originalLocation.startsWith(key)) {
                    String rLoc = getConfigService().getConfig().getTranslator().getGlobalLocationMap().get(key);
                    newLocation = originalLocation.replace(key, rLoc);
                    log.info("Location Map Found. " + key +
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

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    public Boolean translatePartitionLocations(TableMirror tblMirror) {
        Boolean rtn = Boolean.TRUE;
        Config config = getConfigService().getConfig();

//        Config config = EnvironmentConnectionPools.getInstance().getConfig();
        Map<String, String> dbRef = tblMirror.getParent().getDBDefinition(Environment.RIGHT);
        Boolean chkLocation = config.getTransfer().getWarehouse().getManagedDirectory() != null && config.getTransfer().getWarehouse().getExternalDirectory() != null;
        if (config.isEvaluatePartitionLocation()
                && tblMirror.getEnvironmentTable(Environment.LEFT).getPartitioned()
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
                    if (relativeDir.equals(mappedDir) && config.isResetToDefaultLocation()) {
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
                    config.getTranslator().addLocation(getConfigService().getResolvedDB(tblMirror.getParent().getName()), Environment.RIGHT, partitionLocation,
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

    public String translateTableLocation(TableMirror tableMirror, String originalLocation, int level, String partitionSpec) throws Exception {
        String rtn = originalLocation;
        StringBuilder dirBuilder = new StringBuilder();
        String tableName = tableMirror.getName();
        Config config = getConfigService().getConfig();

        String dbName = getConfigService().getResolvedDB(tableMirror.getParent().getName());

//        Config config = Context.getInstance().getConfig();


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
        boolean reMapped = !relativeDir.equals(mappedDir);
        if (reMapped) {
            tableMirror.setReMapped(Boolean.TRUE);
        } else {
            // under conditions like, STORAGE_MIGRATION, same namespace, !rdl and glm we need to ensure ALL locations are
            //   mapped...  If they aren't, they won't be moved as the translation wouldn't change.  So we need to throw
            //   an error that ensures the table fails to process.
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION &&
                    config.getTransfer().getCommonStorage().equals(config.getCluster(Environment.LEFT).getHcfsNamespace()) &&
                    !config.isResetToDefaultLocation()) {
//                tableMirror.addIssue(Environment.LEFT, "Location Mapping can't be determined.  No matching `glm` entry to make translation." +
//                        "Original Location: " + originalLocation);
//                tableMirror.setPhaseState(PhaseState.ERROR);
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
        } else if (config.isResetToDefaultLocation() && config.getTransfer().getWarehouse().getExternalDirectory() != null) {
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

        log.debug("Translate Table Location: " + originalLocation + ": " + dirBuilder);
        // Add Location Map for table to a list.
        // TODO: Need to handle RIGHT locations.
        if (config.getTransfer().getStorageMigration().isDistcp()
                && config.getDataStrategy() != DataStrategyEnum.SQL) {
            if (config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
                config.getTranslator().addLocation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            } else if (config.getTransfer().getStorageMigration().getDataFlow() == DistcpFlow.PULL && !config.isFlip()) {
                config.getTranslator().addLocation(dbName, Environment.RIGHT, originalLocation, dirBuilder.toString().trim(), level);
            } else {
                config.getTranslator().addLocation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            }
        }

        return dirBuilder.toString().trim();
    }

}
