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
public class TranslatorService {

    @Getter
    private HmsMirrorCfgService hmsMirrorCfgService = null;

    /**
     * @param consolidationLevel how far up the directory hierarchy to go to build the distcp list based on the sources
     *                           provided.
     * @return A map of databases.  Each database will have a map that has 1 or more 'targets' and 'x' sources for each
     * target.
     */
    public synchronized Map<String, Map<String, Set<String>>> buildDistcpList(String database, Environment environment, int consolidationLevel) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<>();

        // get the map for a db.
        Set<String> databases = getHmsMirrorCfgService().getHmsMirrorConfig().getTranslator().getDbLocationMap().keySet();

        // get the map.entry
        Map<String, Set<String>> reverseMap = new TreeMap<>();
        // Get a static view of set to avoid concurrent modification.
        Set<EnvironmentMap.TranslationLevel> dbTranslationLevel = new HashSet<>(getHmsMirrorCfgService().getHmsMirrorConfig().getTranslator().getDbLocationMap(database, environment));

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
        partitions.entrySet().stream().forEach(e -> sbPartitionDetails.append("\tPARTITION (")
                .append(e.getKey()).append(") LOCATION '").append(e.getValue()).append("' \n"));
        return sbPartitionDetails.toString();
    }

    public String processGlobalLocationMap(String originalLocation) {
        String newLocation = null;
        if (!getHmsMirrorCfgService().getHmsMirrorConfig().getTranslator().getOrderedGlobalLocationMap().isEmpty()) {
            log.debug("Checking location: {} for replacement element in global location map.", originalLocation);
            for (String key : getHmsMirrorCfgService().getHmsMirrorConfig().getTranslator().getOrderedGlobalLocationMap().keySet()) {
                if (originalLocation.startsWith(key)) {
                    String rLoc = getHmsMirrorCfgService().getHmsMirrorConfig().getTranslator().getOrderedGlobalLocationMap().get(key);
                    newLocation = originalLocation.replace(key, rLoc);
                    log.info("Location Map Found. {}:{} New Location: {}", key, rLoc, newLocation);
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
    public void setHmsMirrorCfgService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
    }

    public Boolean translatePartitionLocations(TableMirror tblMirror) {
        Boolean rtn = Boolean.TRUE;
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

//        Config config = EnvironmentConnectionPools.getInstance().getConfig();
        Map<String, String> dbRef = tblMirror.getParent().getDBDefinition(Environment.RIGHT);
        Boolean chkLocation = hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() != null && hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null;
        if (hmsMirrorConfig.isEvaluatePartitionLocation()
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
                    if (!hmsMirrorConfig.getFilter().isTableFiltering()) {
                        level++;
                    }
                    if (partitionLocation == null || partitionLocation.isEmpty() ||
                            partitionLocation.equals(NOT_SET)) {
                        rtn = Boolean.FALSE;
                        continue;
                    }
                    // Get the relative dir.
                    String relativeDir = partitionLocation.replace(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace(), "");
                    // Check the Global Location Map for a match.
                    String mappedDir = processGlobalLocationMap(relativeDir);
                    if (relativeDir.equals(mappedDir) && hmsMirrorConfig.isResetToDefaultLocation()) {
                        // This is a problem, since we've asked to translate the partitions but didn't find a map, nothing changed.
                        // Which would be inconsistent with the table location details.
                        String errMsg = MessageFormat.format(RDL_W_EPL_NO_MAPPING.getDesc(), entry.getKey(), entry.getValue());
                        tblMirror.addIssue(Environment.RIGHT, errMsg);
                        rtn = Boolean.FALSE;
                    }
                    // Check for 'common storage'
                    String newPartitionLocation = null;
                    if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                        newPartitionLocation = hmsMirrorConfig.getTransfer().getCommonStorage() + mappedDir;
                    } else {
                        newPartitionLocation = hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace() + mappedDir;
                    }
                    entry.setValue(newPartitionLocation);
                    // For distcp.
                    hmsMirrorConfig.getTranslator().addLocation(getHmsMirrorCfgService().getResolvedDB(tblMirror.getParent().getName()), Environment.RIGHT, partitionLocation,
                            newPartitionLocation, ++level);

                    // Check and warn against warehouse locations if specified.
                    if (hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null &&
                            hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() != null) {
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
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        String dbName = getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName());

//        Config config = Context.getInstance().getConfig();


        String leftNS = hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace();
        // Set base on rightNS or Common Storage, if specified
        String rightNS = hmsMirrorConfig.getTransfer().getCommonStorage() == null ?
                hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace() : hmsMirrorConfig.getTransfer().getCommonStorage();

        // Get the relative dir.
        if (!rtn.startsWith(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace())) {
            throw new Exception("Table/Partition Location prefix: `" + originalLocation +
                    "` doesn't match the LEFT clusters defined hcfsNamespace: `" + hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace() +
                    "`. We can't reliably make this translation.");
        }
        String relativeDir = rtn.replace(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace(), "");
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
            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION &&
                    hmsMirrorConfig.getTransfer().getCommonStorage().equals(hmsMirrorConfig.getCluster(Environment.LEFT).getHcfsNamespace()) &&
                    !hmsMirrorConfig.isResetToDefaultLocation()) {
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
        if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
            sbDir.append(hmsMirrorConfig.getTransfer().getCommonStorage());
        } else {
            sbDir.append(rightNS);
        }
        if (reMapped) {
            sbDir.append(mappedDir);
            newLocation = sbDir.toString();
        } else if (hmsMirrorConfig.isResetToDefaultLocation()) {
            // RDL
            if (TableUtils.isManaged(tableMirror.getEnvironmentTable(Environment.LEFT)) && hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory() != null) {
                sbDir.append(hmsMirrorConfig.getTransfer().getWarehouse().getManagedDirectory()).append("/");
                sbDir.append(dbName).append(".db").append("/").append(tableName);
                if (partitionSpec != null)
                    sbDir.append("/").append(partitionSpec);
                newLocation = sbDir.toString();
            } else if (TableUtils.isExternal(tableMirror.getEnvironmentTable(Environment.LEFT)) && hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() != null) {
                sbDir.append(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                sbDir.append(dbName).append(".db").append("/").append(tableName);
                if (partitionSpec != null)
                    sbDir.append("/").append(partitionSpec);
                newLocation = sbDir.toString();
            } else {
                // TODO: Shouldn't happen.
            }
        } else {
            switch (hmsMirrorConfig.getDataStrategy()) {
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

        log.debug("Translate Table Location: {}: {}", originalLocation, dirBuilder);
        // Add Location Map for table to a list.
        // TODO: Need to handle RIGHT locations.
        if (hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()
                && hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.SQL) {
            if (hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION) {
                hmsMirrorConfig.getTranslator().addLocation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            } else if (hmsMirrorConfig.getTransfer().getStorageMigration().getDataFlow() == DistcpFlow.PULL && !hmsMirrorConfig.isFlip()) {
                hmsMirrorConfig.getTranslator().addLocation(dbName, Environment.RIGHT, originalLocation, dirBuilder.toString().trim(), level);
            } else {
                hmsMirrorConfig.getTranslator().addLocation(dbName, Environment.LEFT, originalLocation, dirBuilder.toString().trim(), level);
            }
        }

        return dirBuilder.toString().trim();
    }

}
