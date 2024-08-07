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

import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.util.Protect;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;

import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
public class PasswordService {

    public String decryptPassword(String passwordKey, String decryptPassword) throws EncryptionException {
        Protect protect = new Protect(passwordKey);
        String password = null;
        try {
            if (!isBlank(decryptPassword))
                password = protect.decrypt(decryptPassword);
        } catch (RuntimeException rte) {
            String message = MessageFormat.format("Error decrypting encrypted password: {0} with key: {1}", decryptPassword, passwordKey);
            log.error(message);
            throw new EncryptionException(message, rte);

        }
        return password;
    }

    public String encryptPassword(String passwordKey, String password) throws EncryptionException {
        // Used to generate encrypted password.
        String epassword = null;
        if (!isBlank(passwordKey)) {
            Protect protect = new Protect(passwordKey);
            if (!isBlank(password)) {
                try {
                    epassword = protect.encrypt(password);
                } catch (RuntimeException rte) {
                    String message = MessageFormat.format("Error encrypting password: {0} with password key {1}", password, passwordKey);
                    log.error(message);
                    throw new EncryptionException(message, rte);
                }
            } else {
                // Missing PasswordApp to Encrypt.
                log.error("Missing PasswordApp to Encrypt");
            }
        } else {
            log.error("Missing PasswordApp Key used the encrypt the password");
//            throw new RuntimeException("Missing PasswordApp Key");
        }
        return epassword;
    }

}
