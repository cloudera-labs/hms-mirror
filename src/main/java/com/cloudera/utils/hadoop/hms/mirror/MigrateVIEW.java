/*
 * Copyright (c) 2023. Cloudera, Inc. All Rights Reserved
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

package com.cloudera.utils.hadoop.hms.mirror;

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
