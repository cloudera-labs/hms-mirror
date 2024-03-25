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

import com.cloudera.utils.hms.mirror.Conversion;
import com.cloudera.utils.hms.mirror.Progression;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Getter
@Slf4j
public class ApplicationService {

    private ConfigService configService;
    private Conversion conversion;
    private Progression progression;

    public ApplicationService(ConfigService configService, Conversion conversion, Progression progression) {
        this.configService = configService;
        this.conversion = conversion;
        this.progression = progression;
    }

    public long getReturnCode() {
        long rtn = 0l;
        rtn = getProgression().getErrors().getReturnCode();
        // If app ran, then check for unsuccessful table conversions.
        if (rtn == 0) {
            rtn = getConversion().getUnsuccessfullTableCount();
        }
        return rtn;
    }


}
