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

import com.cloudera.utils.hadoop.hms.stage.Transfer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.*;
import java.util.regex.Matcher;

public class Translator {
    private static Logger LOG = LogManager.getLogger(Translator.class);

    @JsonIgnore
    /**
     * Flag that turns functionality on.
     */
    private boolean on = Boolean.FALSE;

    @JsonIgnore
    private Map<String, Map<String, String>> dbLocationMap = new TreeMap<String, Map<String, String>>();

    /**
     * When distcpCompatible mode is set, there are rules between the rename, consolidation, and location
     * elements in db and tables that we can't support.
     * <p>
     * With distcpCompatible mode enable, rename and location overrides at the table level can't be honored/allowed.
     * <p>
     * When enabled, a sourcelist of directories for a target directory is created.
     */
    private Boolean distcpCompatible = Boolean.TRUE; // default TRUE

    private Map<String, TranslationDatabase> databases;

    public boolean isOn() {
        return on;
    }

    public void setOn(boolean on) {
        this.on = on;
    }

    public Map<String, TranslationDatabase> getDatabases() {
        return databases;
    }

    public void setDatabases(Map<String, TranslationDatabase> databases) {
        this.databases = databases;
    }

    public Boolean getDistcpCompatible() {
        return distcpCompatible;
    }

    public void setDistcpCompatible(Boolean distcpCompatible) {
        this.distcpCompatible = distcpCompatible;
    }

    public Map<String, Map<String, String>> getDbLocationMap() {
        return dbLocationMap;
    }

    public Boolean validate() {
        Boolean rtn = Boolean.TRUE;
        for (Map.Entry<String, TranslationDatabase> entry : databases.entrySet()) {
            TranslationDatabase tdb = entry.getValue();
            if (tdb.getConsolidateExternal() && tdb.getLocation() == null) {
                LOG.error("The 'location' must be set when 'consolidateExternal' is 'true' for: " +
                        entry.getKey());
                rtn = Boolean.FALSE;
            }
            if (tdb.getLocation() != null && !tdb.getLocation().startsWith("/")) {
                LOG.error("The database 'location' must start with a '/' for: " + entry.getKey());
                rtn = Boolean.FALSE;
            }
            if (tdb.getManagedLocation() != null && !tdb.getManagedLocation().startsWith("/")) {
                LOG.error("The database 'managedLocation' must start with a '/' for: " + entry.getKey());
                rtn = Boolean.FALSE;
            }
            if (tdb.getLocation() != null && tdb.getLocation().trim().endsWith("/")) {
                LOG.error("The database 'location' can't end with a '/': " + entry.getKey());
                rtn = Boolean.FALSE;
            }
            if (tdb.getManagedLocation() != null && tdb.getManagedLocation().trim().endsWith("/")) {
                LOG.error("The database 'managedLocation' can't end with a '/': " + entry.getKey());
                rtn = Boolean.FALSE;
            }
            for (Map.Entry<String, TranslationTable> tblEntry : tdb.getTables().entrySet()) {
                TranslationTable ttbl = tblEntry.getValue();
                if (ttbl != null) {
                    if (ttbl.getLocation() != null && (getDistcpCompatible() || tdb.getConsolidateExternal())) {
                        // You can't specify a location for the table with 'distcp' because the destination is
                        // controlled by the sources last folder name.
                        LOG.error("Table 'location' can't be modified when creating a 'distcp' compatible model OR " +
                                "attempting to 'consolidateExternal' tables for: " + entry.getKey() + "." + tblEntry.getKey());
                        rtn = Boolean.FALSE;
                    } else {
                        if (ttbl.getLocation() != null && !ttbl.getLocation().startsWith("/")) {
                            LOG.error("Table 'location' must start with '/' when the translated db is NOT configured for " +
                                    "'consolidateExternal': " + entry.getKey() + "." + tblEntry.getKey());
                            rtn = Boolean.FALSE;
                        }
                    }
                }
            }
        }
        return rtn;
    }

    public String translateDatabase(String database) {
        String rtn = database;
        if (isOn()) {
            TranslationDatabase tdb = getDatabases().get(database);
            if (tdb != null && tdb.getRename() != null) {
                rtn = tdb.getRename();
            }
        }
        return rtn;
    }

    public String translateTable(String database, String table) {
        String rtn = table;
        if (isOn()) {
            TranslationDatabase tdb = getDatabases().get(database);
            if (tdb != null) {
                if (tdb.getTables().get(table) != null && tdb.getTables().get(table).getRename() != null) {
                    rtn = tdb.getTables().get(table).getRename();
                }
            }
        }
        return rtn;
    }


    @JsonIgnore
    private Boolean isDBConsolidateExternal(String database) {
        Boolean rtn = Boolean.FALSE;
        TranslationDatabase tdb = databases.get(database);
        if (tdb != null && tdb.getConsolidateExternal() && tdb.getLocation() != null) {
            rtn = Boolean.TRUE;
        }
        return rtn;
    }

    @JsonIgnore
    private String getDatabaseLocationOverride(String database, String default_) {
        String location = default_;
        TranslationDatabase tdb = databases.get(database);
        if (tdb != null && tdb.getLocation() != null) {
            location = tdb.getLocation();
        }
        return location;
    }

    @JsonIgnore
    private String getTableLocationOverride(String database, String table, String default_) {
        String location = default_;
        TranslationDatabase tdb = databases.get(database);
        if (tdb != null) {
            TranslationTable tbl = tdb.getTables().get(table);
            if (tbl != null && tbl.getLocation() != null) {
                location = tbl.getLocation();
            }
        }
        return location;
    }

    public static String removeLastDirFromUrl(final String url) {
        Matcher matcher = Transfer.lastDirPattern.matcher(url);
        if (matcher.find()) {
            String matchStr = matcher.group(1);
            // Remove last occurrence ONLY.
            Integer lastIndexOf = url.lastIndexOf(matchStr);
            return url.substring(0, lastIndexOf-1);
        } else {
            return url;
        }
    }

    public static String replaceLast(String text, String regex, String replacement) {
        return text.replaceFirst("(?s)"+regex+"(?!.*?"+regex+")", replacement);
    }

    public static String reduceUrlBy(String url, int level) {
        String rtn = url.trim();
        if (rtn.endsWith("/"))
            rtn = rtn.substring(0, rtn.length()-2);
        for (int i=0;i<level;i++) {
             rtn = removeLastDirFromUrl(rtn);
        }
        return rtn;
    }

    public String translateTableLocation(String database, String table, String originalLocation, Config config) {
        String rtn = originalLocation;
        StringBuilder dirBuilder = new StringBuilder();

        String leftNS = config.getCluster(Environment.LEFT).getHcfsNamespace();
        String rightNS = config.getCluster(Environment.RIGHT).getHcfsNamespace();

        if (isOn()) {
            // Get the relative dir.
            String relativeDir = rtn.replace(config.getCluster(Environment.LEFT).getHcfsNamespace(), "");
            TranslationDatabase tdb = getDatabases().get(database);

            // Depending on config, set hcfs prefix
            if (leftNS.trim().endsWith("/")) {
                leftNS.trim().substring(0, leftNS.trim().length() - 2);
            }
            if (rightNS.trim().endsWith("/")) {
                rightNS.trim().substring(0, rightNS.trim().length() - 2);
            }
            switch (config.getDataStrategy()) {
                case DUMP:
                case SCHEMA_ONLY:
                case EXPORT_IMPORT:
                case HYBRID:
                case SQL:
                    dirBuilder.append(rightNS);
                    break;
                case COMMON:
                case LINKED:
//                case INTERMEDIATE:
//                    // We don't need to make adjustments since these strategies share storage
                    dirBuilder.append(leftNS);
                    break;
            }

            // Look for new table location.
            String tblNewLocation = null;
            if (tdb != null) {
                if (tdb.getLocation() != null && tdb.getConsolidateExternal()) {
                    if (getDistcpCompatible()) {
                        // Set to the same name as the current folder
                        dirBuilder.append(tdb.getLocation()).append("/").append(removeLastDirFromUrl(originalLocation));
                    } else {
                        dirBuilder.append(tdb.getLocation()).append("/").append(table);
                    }
                } else if (tdb.getLocation() != null) {
                    if (tdb.getTables().get(table) != null && tdb.getTables().get(table).getLocation() != null) {
                        dirBuilder.append(tdb.getTables().get(table).getLocation());
                    } else {
                        if (getDistcpCompatible()) {
                            dirBuilder.append(tdb.getLocation()).append("/").append(removeLastDirFromUrl(originalLocation));
                        } else {
                            dirBuilder.append(tdb.getLocation()).append("/");
                            if (tdb.getTables().get(table) != null && tdb.getTables().get(table).getRename() != null) {
                                dirBuilder.append(tdb.getTables().get(table).getRename());
                            }
                        }
                    }
                } else if (tdb.getTables().get(table) != null) {
                    if (tdb.getTables().get(table).getLocation() != null) {
                        tblNewLocation = tdb.getTables().get(table).getLocation();
                    }
                } else if (tdb.getLocation() != null) {
                    // Set to the same name as the current folder
                    tblNewLocation = removeLastDirFromUrl(originalLocation);
                }
            }

            if (tdb != null) {
                // Check for consolidation
                if (tdb.getLocation() != null && tdb.getConsolidateExternal()) {
                    dirBuilder.append(tdb.getLocation()).append("/");
                    // If a new location was id'd, use it.  Otherwise use the table name.
                    if (tblNewLocation != null) {
                        dirBuilder.append(tblNewLocation);
                    } else {
                        dirBuilder.append(table);
                    }
                } else if (tdb.getLocation() != null) {
                    dirBuilder.append(tdb.getLocation()).append("/").append(tblNewLocation);
                } else {
                    // If a new location was id'd, use it.  Otherwise use the table name.
                    if (tblNewLocation != null) {
                        dirBuilder.append(tblNewLocation);
                    } else {
                        dirBuilder.append(relativeDir);
                    }
                }
            } else {
                dirBuilder.append(relativeDir);
            }
        } else {
            // Feature Off.  Basic translation.
            String newLocation = null;

            switch (config.getDataStrategy()) {
                case EXPORT_IMPORT:
                case HYBRID:
                case SQL:
                case SCHEMA_ONLY:
                case DUMP:
                    newLocation = originalLocation.replace(leftNS, rightNS);
                    break;
                case LINKED:
                case COMMON:
                    newLocation = originalLocation;
                    break;
            }
            dirBuilder.append(newLocation);
        }

        LOG.debug("Translate Table Location: " + originalLocation + ": " + dirBuilder.toString());
        // Add Location Map for table to a list.
        Map<String, String> locationMap = dbLocationMap.get(database);
        if (locationMap == null) {
            locationMap = new HashMap<String, String>();
            getDbLocationMap().put(database, locationMap);
        }

        locationMap.put(originalLocation, dirBuilder.toString().trim());

        return dirBuilder.toString().trim();
    }

    /**
     * @param consolidationLevel how far up the directory hierarchy to go to build the distcp list based on the sources
     *                           provided.
     * @return A map of databases.  Each database will have a map that has 1 or more 'targets' and 'x' sources for each
     * target.
     */
    public Map<String, Map<String, Set<String>>> buildDistcpList(int consolidationLevel) {
        Map<String, Map<String, Set<String>>> rtn = new TreeMap<String, Map<String, Set<String>>>();
        // dbLocationMap Map<String, Map<String, String>>

        // get the map for a db.
        Set<String> databases = dbLocationMap.keySet();

        for (String database: databases) {
            // get the map.entry
            Map<String, Set<String>> reverseMap = new TreeMap<String, Set<String>>();
            Map<String, String> dbMap = dbLocationMap.get(database);
            for (Map.Entry<String, String> entry : dbMap.entrySet()) {
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
        }
        return rtn;
    }

}
