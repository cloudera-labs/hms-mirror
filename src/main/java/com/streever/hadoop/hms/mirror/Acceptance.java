package com.streever.hadoop.hms.mirror;

public class Acceptance {

    private Boolean silentOverride;
    private Boolean backupOfHDFS;
    private Boolean backupOfMetastore;
    private Boolean trashConfigure;

    public Boolean getSilentOverride() {
        return silentOverride;
    }

    public void setSilentOverride(Boolean silentOverride) {
        this.silentOverride = silentOverride;
    }

    public Boolean getBackupOfHDFS() {
        return backupOfHDFS;
    }

    public void setBackupOfHDFS(Boolean backupOfHDFS) {
        this.backupOfHDFS = backupOfHDFS;
    }

    public Boolean getBackupOfMetastore() {
        return backupOfMetastore;
    }

    public void setBackupOfMetastore(Boolean backupOfMetastore) {
        this.backupOfMetastore = backupOfMetastore;
    }

    public Boolean getTrashConfigure() {
        return trashConfigure;
    }

    public void setTrashConfigure(Boolean trashConfigure) {
        this.trashConfigure = trashConfigure;
    }
}
