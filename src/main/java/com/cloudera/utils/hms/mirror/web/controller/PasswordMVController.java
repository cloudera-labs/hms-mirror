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
import com.cloudera.utils.hms.mirror.service.ExecuteSessionService;
import com.cloudera.utils.hms.mirror.service.PasswordService;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import static com.cloudera.utils.hms.mirror.web.controller.ControllerReferences.PASSWORDS;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Controller
@RequestMapping(path = "/password")
@Slf4j
public class PasswordMVController {

    private ExecuteSessionService executeSessionService;
    private PasswordService passwordService;
    private UIModelService uiModelService;

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setPasswordService(PasswordService passwordService) {
        this.passwordService = passwordService;
    }

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @RequestMapping(value = "/view", method = RequestMethod.GET)
    public String view(Model model) {
        // From View Page of Config, collect passwords and check them for
        //   decrypt and encrypt.  Doesn't save them, just tests.
        // Get current config.
        ExecuteSession executeSession = executeSessionService.getSession();
        HmsMirrorConfig config = executeSession.getConfig();
        PasswordContainer passwordContainer = new PasswordContainer();
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
        model.addAttribute(PASSWORDS, passwordContainer);
        uiModelService.sessionToModel(model, 1, Boolean.FALSE);
        return "password/view";
    }

    @RequestMapping(value = "/decrypt", method = RequestMethod.POST)
    public String decrypt(Model model,
                                @ModelAttribute(PASSWORDS) PasswordContainer passwordContainer) throws EncryptionException, RequiredConfigurationException {
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
        model.addAttribute(PASSWORDS, newPasswordContainer);
        return "password/view";
    }

    @RequestMapping(value = "/encrypt", method = RequestMethod.POST)
    public String encrypt(Model model,
                          @ModelAttribute(PASSWORDS) PasswordContainer passwordContainer) throws EncryptionException, RequiredConfigurationException {
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
        model.addAttribute(PASSWORDS, newPasswordContainer);
        return "password/view";
    }

}
