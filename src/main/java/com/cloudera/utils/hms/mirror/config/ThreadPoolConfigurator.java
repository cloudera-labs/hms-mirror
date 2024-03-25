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

package com.cloudera.utils.hms.mirror.config;

import com.cloudera.utils.hms.mirror.service.ConfigService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadPoolConfigurator {

    @Bean("jobThreadPool")
    @Order(20)
    public TaskExecutor jobThreadPool(ConfigService configService) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setMaxPoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("job-");
        executor.initialize();
        return executor;
    }

    @Bean("metadataThreadPool")
    @Order(20)
    public TaskExecutor metadataThreadPool(ConfigService configService) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setMaxPoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("metadata-");
        executor.initialize();
        return executor;
    }

    @Bean("reportingThreadPool")
    @Order(20)
    public TaskExecutor reportingThreadPool(ConfigService configService) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setMaxPoolSize(configService.getConfig().getTransfer().getConcurrency());
        executor.setQueueCapacity(Integer.MAX_VALUE);
        executor.setThreadNamePrefix("reporting-");
        executor.initialize();
        return executor;
    }

}
