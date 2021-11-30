package com.cloudera.utils.hadoop.hms.mirror;

public class TranslationTable {

    /**
     * Optional, when defined, rename the table.
     *
     * The rename WILL affect the location IF:
     * - the location is NOT specified AND your NOT running in distcpCompatible mode.
     *
     * When the location is not specified, a rename will use the new standard location as
     * a basis for the location.
     */
    private String rename;
    /*
    Optional, when specified, this will override all other hierarchy
    attempts to set this.
     */
    private String location;

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
}
