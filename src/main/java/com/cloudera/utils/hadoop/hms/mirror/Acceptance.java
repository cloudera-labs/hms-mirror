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
