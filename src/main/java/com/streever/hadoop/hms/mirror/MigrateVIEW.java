package com.streever.hadoop.hms.mirror;

public class MigrateVIEW {

    /*
    Whether or not we'll be migrating VIEWs.
    */
    private Boolean on = Boolean.FALSE;

//    private Boolean dropFirst = Boolean.FALSE;

    public Boolean isOn() {
        return on;
    }

    public void setOn(Boolean on) {
        this.on = on;
    }

//    public Boolean isDropFirst() {
//        return dropFirst;
//    }
//
//    public void setDropFirst(Boolean dropFirst) {
//        this.dropFirst = dropFirst;
//    }
}
