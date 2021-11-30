package com.cloudera.utils.hadoop.hms.mirror;

import java.util.Map;

public class TranslationDatabase {
    /*
    Optional, when defined, rename the db to this new name.
     */
    private String rename;
    /*
    Optional, when defined, set the db location.
     */
    private String location;
    /*
    Optional, when defined, set the db managed location.
    only relevant for non-legacy environments.
     */
    private String managedLocation;
    /*
    For all external (hive3) or legacy managed we need to pull
    all the data under the defined db location.
     */
    private Boolean consolidateExternal = Boolean.FALSE;
    /*
    Optional, map of tables and directives to apply.  When a
    table isn't defined here, nothing will be done from a
    translation perspective.
     */
    private Map<String, TranslationTable> tables;

    public String getRename() {
        return rename;
    }

    public void setRename(String rename) {
        this.rename = rename;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getManagedLocation() {
        return managedLocation;
    }

    public void setManagedLocation(String managedLocation) {
        this.managedLocation = managedLocation;
    }

    public Boolean getConsolidateExternal() {
        return consolidateExternal;
    }

    public void setConsolidateExternal(Boolean consolidateExternal) {
        this.consolidateExternal = consolidateExternal;
    }

    public Map<String, TranslationTable> getTables() {
        return tables;
    }

    public void setTables(Map<String, TranslationTable> tables) {
        this.tables = tables;
    }
}
