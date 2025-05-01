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

package com.cloudera.utils.hms.mirror.web.controller;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.PasswordContainer;
import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.PASSWORD_CONTAINER;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Controller
@RequestMapping(path = "/password")
@Slf4j
public class PasswordMVController {

    private final ExecuteSessionService executeSessionService;
    private final PasswordService passwordService;
    private final UIModelService uiModelService;

    public PasswordMVController(
            ExecuteSessionService executeSessionService,
            PasswordService passwordService,
            UIModelService uiModelService
    ) {
        this.executeSessionService = executeSessionService;
        this.passwordService = passwordService;
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String view(Model model,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        // From View Page of Config, collect passwords and check them for
        //   decrypt and encrypt.  Doesn't save them, just tests.
        // Get current config.
        ExecuteSession executeSession = executeSessionService.getSession();
        HmsMirrorConfig config = executeSession.getConfig();
        PasswordContainer passwordContainer = new PasswordContainer();
        passwordContainer.setPasswordKey(config.getPasswordKey());
        passwordContainer.setEncrypted(config.isEncryptedPasswords());
        if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
            passwordContainer.setLeftHS2(config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
        }
        if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
            passwordContainer.setRightHS2(config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
        }
        if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
            passwordContainer.setLeftMetastore(config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
        }
        if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
            passwordContainer.setRightMetastore(config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
        }
        model.addAttribute(PASSWORD_CONTAINER, passwordContainer);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "password/view";
    }

    @RequestMapping(value = "/reveal", method = RequestMethod.GET)
    public String reveal(Model model,
                         @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        // From View Page of Config, collect passwords and check them for
        //   decrypt and encrypt.  Doesn't save them, just tests.
        // Get current config.
        ExecuteSession executeSession = executeSessionService.getSession();
        HmsMirrorConfig config = executeSession.getConfig();
        PasswordContainer passwordContainer = new PasswordContainer();
        passwordContainer.setPasswordKey(config.getPasswordKey());
        passwordContainer.setEncrypted(config.isEncryptedPasswords());
        if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
            passwordContainer.setLeftHS2(config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
        }
        if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
            passwordContainer.setRightHS2(config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
        }
        if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
            passwordContainer.setLeftMetastore(config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
        }
        if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
            passwordContainer.setRightMetastore(config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
        }
        model.addAttribute(PASSWORD_CONTAINER, passwordContainer);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "password/reveal";
    }

    @RequestMapping(value = "/setPasskey", method = RequestMethod.POST)
    public String view(Model model,
                       @ModelAttribute(PASSWORD_CONTAINER) PasswordContainer container,
                       @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) {
        // From View Page of Config, collect passwords and check them for
        //   decrypt and encrypt.  Doesn't save them, just tests.
        // Get current config.
        ExecuteSession executeSession = executeSessionService.getSession();
        HmsMirrorConfig config = executeSession.getConfig();
        config.setPasswordKey(container.getPasswordKey());
        // should retain the other values.
        model.addAttribute(PASSWORD_CONTAINER, container);
        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);
        return "password/view";
    }

        @RequestMapping(value = "/doEncryptPasswords", method = RequestMethod.POST)
    public String doEncryptPasswords(Model model,
                                     @ModelAttribute(PASSWORD_CONTAINER) PasswordContainer container,
                                     @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException, EncryptionException {
        executeSessionService.closeSession();

        ExecuteSession session = executeSessionService.getSession();

        // Work with the current session config.  All the values should've been saved already to allow this.
        HmsMirrorConfig config = session.getConfig();

        config.setPasswordKey(container.getPasswordKey());

        if (!config.isEncryptedPasswords() && !isBlank(config.getPasswordKey()))  {
            String passwordKey = config.getPasswordKey();
            String lhs2 = null;
            String lms = null;
            String rhs2 = null;
            String rms = null;
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
                lhs2 = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                rhs2 = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                lms = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                rms = passwordService.encryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            config.setEncryptedPasswords(Boolean.TRUE);
            if (nonNull(lhs2)) {
                config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().setProperty("password", lhs2);
            }
            if (nonNull(rhs2)) {
                config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().setProperty("password", rhs2);
            }
            if (nonNull(lms)) {
                config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().setProperty("password", lms);
            }
            if (nonNull(rms)) {
                config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().setProperty("password", rms);
            }
        } else {
            throw new SessionException("Inconsistent state: Encrypted, Password Key, etc. . Can't encrypt.");
        }

        session.resetConnectionStatuses();

        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);

        return "/config/view";
    }

    @RequestMapping(value = "/doDecryptPasswords", method = RequestMethod.POST)
    public String doDecryptPasswords(Model model,
                                     @ModelAttribute(PASSWORD_CONTAINER) PasswordContainer container,
                                     @Value("${hms-mirror.concurrency.max-threads}") Integer maxThreads) throws SessionException, EncryptionException {
        executeSessionService.closeSession();

        ExecuteSession session = executeSessionService.getSession();

        // Work with the current session config.  All the values should've been saved already to allow this.
        HmsMirrorConfig config = session.getConfig();

        // Set the config password key to the password key in the container
        config.setPasswordKey(container.getPasswordKey());

        if (config.isEncryptedPasswords() && !isBlank(config.getPasswordKey())) {
            String passwordKey = config.getPasswordKey();
            String lhs2 = null;
            String lms = null;
            String rhs2 = null;
            String rms = null;
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getHiveServer2())) {
                lhs2 = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getHiveServer2())) {
                rhs2 = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.LEFT)) && nonNull(config.getCluster(Environment.LEFT).getMetastoreDirect())) {
                lms = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            if (nonNull(config.getCluster(Environment.RIGHT)) && nonNull(config.getCluster(Environment.RIGHT).getMetastoreDirect())) {
                rms = passwordService.decryptPassword(passwordKey, config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().getProperty("password"));
            }
            config.setEncryptedPasswords(Boolean.FALSE);
            if (nonNull(lhs2)) {
                config.getCluster(Environment.LEFT).getHiveServer2().getConnectionProperties().setProperty("password", lhs2);
            }
            if (nonNull(rhs2)) {
                config.getCluster(Environment.RIGHT).getHiveServer2().getConnectionProperties().setProperty("password", rhs2);
            }
            if (nonNull(lms)) {
                config.getCluster(Environment.LEFT).getMetastoreDirect().getConnectionProperties().setProperty("password", lms);
            }
            if (nonNull(rms)) {
                config.getCluster(Environment.RIGHT).getMetastoreDirect().getConnectionProperties().setProperty("password", rms);
            }
        } else {
            throw new SessionException("Inconsistent state. Encrypted, PasswordKey, etc. . Can't decrypt.");
        }

        session.resetConnectionStatuses();

        uiModelService.sessionToModel(model, maxThreads, Boolean.FALSE);

        return "/config/view";
    }

    @RequestMapping(value = "/decrypt", method = RequestMethod.POST)
    public String decrypt(Model model,
                                @ModelAttribute(PASSWORD_CONTAINER) PasswordContainer passwordContainer) throws EncryptionException, RequiredConfigurationException {
        PasswordContainer newPasswordContainer = new PasswordContainer();

        if (isNull(passwordContainer.getPasswordKey()) || passwordContainer.getPasswordKey().isEmpty()) {
            throw new RequiredConfigurationException("Need to specify password key");
        }
        if (nonNull(passwordContainer.getLeftHS2()) && !passwordContainer.getLeftHS2().isEmpty()) {
            newPasswordContainer.setLeftHS2(passwordService.decryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getLeftHS2()));
        }
        if (nonNull(passwordContainer.getRightHS2())  && !passwordContainer.getRightHS2().isEmpty()) {
            newPasswordContainer.setRightHS2(passwordService.decryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getRightHS2()));
        }
        if (nonNull(passwordContainer.getLeftMetastore()) && !passwordContainer.getLeftMetastore().isEmpty()) {
            newPasswordContainer.setLeftMetastore(passwordService.decryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getLeftMetastore()));
        }
        if (nonNull(passwordContainer.getRightMetastore()) && !passwordContainer.getRightMetastore().isEmpty()) {
            newPasswordContainer.setRightMetastore(passwordService.decryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getRightMetastore()));
        }
        if (nonNull(passwordContainer.getTestPassword()) && !passwordContainer.getTestPassword().isEmpty()) {
            newPasswordContainer.setTestPassword(passwordService.decryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getTestPassword()));
        }
        uiModelService.sessionToModel(model, 0, Boolean.FALSE);
        model.addAttribute(PASSWORD_CONTAINER, newPasswordContainer);
        return "password/reveal";
    }

    @RequestMapping(value = "/encrypt", method = RequestMethod.POST)
    public String encrypt(Model model,
                          @ModelAttribute(PASSWORD_CONTAINER) PasswordContainer passwordContainer) throws EncryptionException, RequiredConfigurationException {
        PasswordContainer newPasswordContainer = new PasswordContainer();

        if (isNull(passwordContainer.getPasswordKey()) || passwordContainer.getPasswordKey().isEmpty()) {
            throw new RequiredConfigurationException("Need to specify password key");
        }
        if (nonNull(passwordContainer.getLeftHS2()) && !passwordContainer.getLeftHS2().isEmpty()) {
            newPasswordContainer.setLeftHS2(passwordService.encryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getLeftHS2()));
        }
        if (nonNull(passwordContainer.getRightHS2())  && !passwordContainer.getRightHS2().isEmpty()) {
            newPasswordContainer.setRightHS2(passwordService.encryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getRightHS2()));
        }
        if (nonNull(passwordContainer.getLeftMetastore()) && !passwordContainer.getLeftMetastore().isEmpty()) {
            newPasswordContainer.setLeftMetastore(passwordService.encryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getLeftMetastore()));
        }
        if (nonNull(passwordContainer.getRightMetastore()) && !passwordContainer.getRightMetastore().isEmpty()) {
            newPasswordContainer.setRightMetastore(passwordService.encryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getRightMetastore()));
        }
        if (nonNull(passwordContainer.getTestPassword()) && !passwordContainer.getTestPassword().isEmpty()) {
            newPasswordContainer.setTestPassword(passwordService.encryptPassword(
                    passwordContainer.getPasswordKey(), passwordContainer.getTestPassword()));
        }
        uiModelService.sessionToModel(model, 0, Boolean.FALSE);
        model.addAttribute(PASSWORD_CONTAINER, newPasswordContainer);
        return "password/reveal";
    }

}