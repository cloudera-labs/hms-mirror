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

package com.cloudera.utils.hms.mirror.web.config;

import com.cloudera.utils.hms.mirror.exceptions.EncryptionException;
import com.cloudera.utils.hms.mirror.exceptions.MismatchException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import com.cloudera.utils.hms.mirror.service.UIModelService;
import com.cloudera.utils.hms.mirror.web.controller.ControllerReferences;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;

@ControllerAdvice
public class MirrorExceptionHandler {

    private UIModelService uiModelService;

    @Autowired
    public void setUiModelService(UIModelService uiModelService) {
        this.uiModelService = uiModelService;
    }

    @ExceptionHandler(value = SessionException.class)
    public ModelAndView sessionExceptionHandler(HttpServletRequest request, SessionException exception) {
        ModelAndView mv = new ModelAndView();

        mv.addObject(ControllerReferences.TYPE, "Session Exception");
        mv.addObject(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(mv, 0, false);
        mv.setViewName("error");
        return mv;
    }

    @ExceptionHandler(value = RequiredConfigurationException.class)
    public ModelAndView reqConfigExceptionHandler(HttpServletRequest request, RequiredConfigurationException exception) {
        ModelAndView mv = new ModelAndView();

        mv.getModel().put(ControllerReferences.TYPE, "Required Configuration");
        mv.getModel().put(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(mv.getModel(), 0, false);
        mv.setViewName("error");
        return mv;
    }

//    @ExceptionHandler(value = EncryptionException.class)
//    public ModelAndView encryptionExceptionHandler(HttpServletRequest request, EncryptionException exception) {
//        ModelAndView mv = new ModelAndView();
//
//        mv.getModel().put(ControllerReferences.TYPE, "Encryption/Decryption Issue");
//        mv.getModel().put(ControllerReferences.MESSAGE, exception.getMessage());
//        uiModelService.sessionToModel(mv.getModel(), 0, false);
//        mv.setViewName("error");
//        return mv;
//    }

    @ExceptionHandler(value = EncryptionException.class)
    public String encryptionExceptionHandler(Model model, EncryptionException exception) {
        model.addAttribute(ControllerReferences.TYPE, "Password Encryption/Decryption Issue");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

    @ExceptionHandler(value = MismatchException.class)
    public String misMatchExceptionHandler(Model model, MismatchException exception) {
        model.addAttribute(ControllerReferences.TYPE, "Mismatch Issue");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }


    @ExceptionHandler(value = IOException.class)
    public String ioExceptionHandler(Model model, IOException exception) {
        model.addAttribute(ControllerReferences.TYPE, "IO Exception Issue");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

    @ExceptionHandler(value = UnknownHostException.class)
    public String unKnownHostHandler(Model model, UnknownHostException exception) {
        model.addAttribute(ControllerReferences.TYPE, "Unknown Host Issue");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

    @ExceptionHandler(value = SQLException.class)
    public String sqlExceptionHandler(Model model, SQLException exception) {
        model.addAttribute(ControllerReferences.TYPE, "SQL Exception Issue");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

    @ExceptionHandler(value = SQLInvalidAuthorizationSpecException.class)
    public String SQLInvalidAuthorizationSpecExceptionHandler(Model model, SQLInvalidAuthorizationSpecException exception) {
        model.addAttribute(ControllerReferences.TYPE, "SQL Invalid Auth Exception Issue");
        model.addAttribute(ControllerReferences.MESSAGE, exception.getMessage());
        uiModelService.sessionToModel(model, 0, false);
        return "error";
    }

}
