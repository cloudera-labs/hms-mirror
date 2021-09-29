package com.streever.hadoop.hms.mirror;

public enum Environment {
    LEFT(Boolean.TRUE),
    RIGHT(Boolean.TRUE),
    /*
    Table lives on RIGHT cluster and usually points to LEFT data.  Used to migrate
    data from the LEFT to the RIGHT.  Temp table and should be deleted after action.
    Should not own data.
     */
    SHADOW(Boolean.FALSE),
    /*
    Table lives on LEFT cluster and will own the data.  This is usually where we'd move
    ACID table "data" to (EXTERNAL table) so we can attach to it via "SHADOW" table on RIGHT
    cluster and finish migration to final RIGHT table.
     */
    TRANSFER(Boolean.FALSE),
    /*
    Abstractions from LEFT and RIGHT for use internally.
     */
    SOURCE(Boolean.FALSE),
    TARGET(Boolean.FALSE);

    private Boolean visible;

    Environment(Boolean visible) {
        this.visible = visible;
    }

    public Boolean isVisible() {
        return visible;
    }
}
