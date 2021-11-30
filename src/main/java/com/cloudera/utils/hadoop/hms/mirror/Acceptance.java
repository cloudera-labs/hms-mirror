package com.cloudera.utils.hadoop.hms.mirror;

public class Acceptance {

    private Boolean silentOverride;
    private Boolean backedUpHDFS;
    private Boolean backedUpMetastore;
    private Boolean trashConfigured;
    private Boolean potentialDataLoss;

    public Boolean getSilentOverride() {
        return silentOverride;
    }

    public void setSilentOverride(Boolean silentOverride) {
        this.silentOverride = silentOverride;
    }

    public Boolean getBackedUpHDFS() {
        return backedUpHDFS;
    }

    public void setBackedUpHDFS(Boolean backedUpHDFS) {
        this.backedUpHDFS = backedUpHDFS;
    }

    public Boolean getBackedUpMetastore() {
        return backedUpMetastore;
    }

    public void setBackedUpMetastore(Boolean backedUpMetastore) {
        this.backedUpMetastore = backedUpMetastore;
    }

    public Boolean getTrashConfigured() {
        return trashConfigured;
    }

    public void setTrashConfigured(Boolean trashConfigured) {
        this.trashConfigured = trashConfigured;
    }

    public Boolean getPotentialDataLoss() {
        return potentialDataLoss;
    }

    public void setPotentialDataLoss(Boolean potentialDataLoss) {
        this.potentialDataLoss = potentialDataLoss;
    }
}
