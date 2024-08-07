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

package com.cloudera.utils.hms.mirror.encryption;

import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.password.Password;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@Getter
@ActiveProfiles("no-cli")
public class PasswordTestBase {

    protected Password password;
    protected PasswordService passwordService = new PasswordService();

    @Autowired
    public void setPassword(Password password) {
        this.password = password;
    }

    public String doIt() throws EncryptionException {
        String value = null;
        switch (password.getConversion()) {
            case DECRYPT:
                value = passwordService.decryptPassword(password.getPasswordKey(), password.getValue());
                break;
            case ENCRYPT:
                value = passwordService.encryptPassword(password.getPasswordKey(), password.getValue());
                break;
        }
        return value;
    }

}
