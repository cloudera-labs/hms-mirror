/*
 * Copyright (c) 2023-2024. Cloudera, Inc. All Rights Reserved
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

import com.cloudera.utils.hms.mirror.domain.EnvironmentTable;
import com.cloudera.utils.hms.mirror.PhaseState;
import com.cloudera.utils.hms.mirror.connections.ConnectionException;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategy;
import com.cloudera.utils.hms.mirror.datastrategy.HybridAcidDowngradeInPlaceDataStrategy;
import com.cloudera.utils.hms.mirror.datastrategy.HybridDataStrategy;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.TableMirror;
import com.cloudera.utils.hms.mirror.domain.Warehouse;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.HmsMirrorConfigUtil;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import com.cloudera.utils.hms.stage.ReturnStatus;
import com.cloudera.utils.hms.util.TableUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.cloudera.utils.hms.mirror.MessageCode.DISTCP_FOR_SO_ACID;
import static org.apache.commons.lang3.StringUtils.isBlank;

@Service
@Slf4j
@Getter
@Setter
public class TransferService {
    public static Pattern protocolNSPattern = Pattern.compile("(^.*://)([a-zA-Z0-9](?:(?:[a-zA-Z0-9-]*|(?<!-)\\.(?![-.]))*[a-zA-Z0-9]+)?)(:\\d{4})?");
    // Pattern to find the value of the last directory in a url.
    public static Pattern lastDirPattern = Pattern.compile(".*/([^/?]+).*");
    private final DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
    private final DateFormat tdf = new SimpleDateFormat("HH:mm:ss.SSS");
    private ConfigService configService;
    private ExecuteSessionService executeSessionService;
    private TableService tableService;
    private DatabaseService databaseService;
    private DataStrategyService dataStrategyService;
    private HybridDataStrategy hybridDataStrategy;
    private HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy;

    @Autowired
    public void setConfigService(ConfigService configService) {
        this.configService = configService;
    }

    @Autowired
    public void setExecuteSessionService(ExecuteSessionService executeSessionService) {
        this.executeSessionService = executeSessionService;
    }

    @Autowired
    public void setDataStrategyService(DataStrategyService dataStrategyService) {
        this.dataStrategyService = dataStrategyService;
    }

    @Autowired
    public void setDatabaseService(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Autowired
    public void setHybridAcidDowngradeInPlaceDataStrategy(HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy) {
        this.hybridAcidDowngradeInPlaceDataStrategy = hybridAcidDowngradeInPlaceDataStrategy;
    }

    @Autowired
    public void setHybridDataStrategy(HybridDataStrategy hybridDataStrategy) {
        this.hybridDataStrategy = hybridDataStrategy;
    }

    @Autowired
    public void setTableService(TableService tableService) {
        this.tableService = tableService;
    }

    @Async("jobThreadPool")
    public Future<ReturnStatus> transfer(TableMirror tableMirror) {
        ReturnStatus rtn = new ReturnStatus();
//        rtn.setStatus(ReturnStatus.Status.SUCCESS);
//        Boolean successful = Boolean.FALSE;
        HmsMirrorConfig config = executeSessionService.getSession().getConfig();
        RunStatus runStatus = executeSessionService.getSession().getRunStatus();

        Warehouse warehouse = null;
        try {
            Date start = new Date();
            log.info("Migrating {}.{}", tableMirror.getParent().getName(), tableMirror.getName());

            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);


            // Set Database to Transfer DB.
            tableMirror.setPhaseState(PhaseState.STARTED);

            tableMirror.setStrategy(config.getDataStrategy());
//            tblMirror.setResolvedDbName(config.getResolvedDB(tblMirror.getParent().getName()));

            tableMirror.incPhase();
            tableMirror.addStep("TRANSFER", config.getDataStrategy().toString());
            try {
                DataStrategy dataStrategy = null;
                switch (config.getDataStrategy()) {
                    case HYBRID:
                        if (TableUtils.isACID(let) && config.getMigrateACID().isDowngradeInPlace()) {
                            if (hybridAcidDowngradeInPlaceDataStrategy.execute(tableMirror)) {
                                rtn.setStatus(ReturnStatus.Status.SUCCESS);
                            } else {
                                rtn.setStatus(ReturnStatus.Status.ERROR);
                                runStatus.getOperationStatistics().getIssues().incrementTables();
                            }
                        } else {
                            if (hybridDataStrategy.execute(tableMirror)) {
                                rtn.setStatus(ReturnStatus.Status.SUCCESS);
                            } else {
                                rtn.setStatus(ReturnStatus.Status.ERROR);
                            }
                        }
                        break;
                    default:
                        dataStrategy = getDataStrategyService().getDefaultDataStrategy(config);
                        if (dataStrategy.execute(tableMirror)) {
                            rtn.setStatus(ReturnStatus.Status.SUCCESS);
                        } else {
                            rtn.setStatus(ReturnStatus.Status.ERROR);
                        }
                        break;
                }
                // Build out DISTCP workplans.
                boolean consolidateSourceTables = config.getTransfer().getStorageMigration().isConsolidateTablesForDistcp();

                if (rtn.getStatus() == ReturnStatus.Status.SUCCESS && config.getTransfer().getStorageMigration().isDistcp()) {
                    warehouse = databaseService.getWarehousePlan(tableMirror.getParent().getName());
                    // Build distcp reports.
                    if (!isBlank(config.getTransfer().getIntermediateStorage())) {
                        // LEFT PUSH INTERMEDIATE
                        // The Transfer Table should be available.
                        String isLoc = config.getTransfer().getIntermediateStorage();
                        // Deal with extra '/'
                        isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                        isLoc = isLoc + "/" +
                                config.getTransfer().getRemoteWorkingDirectory() + "/" +
                                config.getRunMarker() + "/" +
                                tableMirror.getParent().getName() + ".db/" +
                                tableMirror.getName();

                        config.getTranslator().addTranslation(tableMirror.getParent().getName(), Environment.LEFT,
                                TableUtils.getLocation(tableMirror.getName(), let.getDefinition()),
                                isLoc, 1, consolidateSourceTables);
                        // RIGHT PULL from INTERMEDIATE
                        String fnlLoc = null;
                        if (!set.getDefinition().isEmpty()) {
                            fnlLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                        } else {
                            fnlLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (isBlank(fnlLoc) && config.loadMetadataDetails()) {
                                StringBuilder sbDir = new StringBuilder();
                                sbDir.append(config.getTargetNamespace());
//                                if (!isBlank(config.getTransfer().getTargetNamespace())) {
//                                    sbDir.append(config.getTransfer().getTargetNamespace());
//                                } else {
//                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
//                                }
                                sbDir.append(warehouse.getExternalDirectory()).append("/");
                                sbDir.append(HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config)).append(".db").append("/").append(tableMirror.getName());
                                fnlLoc = sbDir.toString();
                            }
                        }
                        config.getTranslator().addTranslation(tableMirror.getParent().getName(), Environment.RIGHT,
                                isLoc,
                                fnlLoc, 1, consolidateSourceTables);
                    } else if (!isBlank(config.getTransfer().getTargetNamespace())
                            && config.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
                        // LEFT PUSH COMMON
                        String origLoc = TableUtils.isACID(let) ?
                                TableUtils.getLocation(let.getName(), tet.getDefinition()) :
                                TableUtils.getLocation(let.getName(), let.getDefinition());
                        String newLoc = null;
                        if (TableUtils.isACID(let)) {
                            if (config.getMigrateACID().isDowngrade()) {
                                newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                            } else {
                                newLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                            }
                        } else {
                            newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                        }
                        if (isBlank(newLoc) && config.loadMetadataDetails()) {
                            String sbDir = config.getTransfer().getTargetNamespace() +
                                    warehouse.getExternalDirectory() + "/" +
                                    HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config) + ".db" + "/" + tableMirror.getName();
                            newLoc = sbDir;
                        }
                        config.getTranslator().addTranslation(tableMirror.getParent().getName(), Environment.LEFT,
                                origLoc, newLoc, 1, consolidateSourceTables);
                    } else {
                        // RIGHT PULL
                        if (TableUtils.isACID(let)
                                && !config.getMigrateACID().isDowngrade()
                                && !(config.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
                            tableMirror.addIssue(Environment.RIGHT, DISTCP_FOR_SO_ACID.getDesc());
                            rtn.setStatus(ReturnStatus.Status.ERROR);
//                            successful = Boolean.FALSE;
                        } else if (TableUtils.isACID(let) && config.getMigrateACID().isDowngrade()) {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (isBlank(rLoc) && config.loadMetadataDetails()) {
                                StringBuilder sbDir = new StringBuilder();
                                sbDir.append(config.getTargetNamespace());
//                                if (!isBlank(config.getTransfer().getTargetNamespace())) {
//                                    sbDir.append(config.getTransfer().getTargetNamespace());
//                                } else {
//                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
//                                }
                                sbDir.append(warehouse.getExternalDirectory()).append("/");
                                sbDir.append(HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config)).append(".db").append("/").append(tableMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            config.getTranslator().addTranslation(tableMirror.getParent().getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), tet.getDefinition()),
                                    rLoc, 1, consolidateSourceTables);
                        } else {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (isBlank(rLoc) && config.loadMetadataDetails()) {
                                StringBuilder sbDir = new StringBuilder();
                                sbDir.append(config.getTargetNamespace());
//                                if (!isBlank(config.getTransfer().getTargetNamespace())) {
//                                    sbDir.append(config.getTransfer().getTargetNamespace());
//                                } else {
//                                    sbDir.append(config.getCluster(Environment.RIGHT).getHcfsNamespace());
//                                }
                                sbDir.append(warehouse.getExternalDirectory()).append("/");
                                sbDir.append(HmsMirrorConfigUtil.getResolvedDB(tableMirror.getParent().getName(), config)).append(".db").append("/").append(tableMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            config.getTranslator().addTranslation(tableMirror.getParent().getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), let.getDefinition())
                                    , rLoc, 1, consolidateSourceTables);
                        }
                    }
                }

                if (rtn.getStatus() == ReturnStatus.Status.SUCCESS)
                    tableMirror.setPhaseState(PhaseState.SUCCESS);
                else
                    tableMirror.setPhaseState(PhaseState.ERROR);
            } catch (ConnectionException ce) {
                tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + ce.getMessage());
                log.error("Connection Error", ce);
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(ce);
            } catch (RuntimeException rte) {
                tableMirror.addIssue(Environment.LEFT, "FAILURE (check logs):" + rte.getMessage());
                log.error("Transfer Error", rte);
                rtn.setStatus(ReturnStatus.Status.FATAL);
                rtn.setException(rte);
            }

            Date end = new Date();
            Long diff = end.getTime() - start.getTime();
            tableMirror.setStageDuration(diff);
            log.info("Migration complete for {}.{} in {}ms", tableMirror.getParent().getName(), tableMirror.getName(), diff);
        } catch (MissingDataPointException | RequiredConfigurationException mde) {
            rtn.setStatus(ReturnStatus.Status.FATAL);
            rtn.setException(mde);
        }
        return new AsyncResult<>(rtn);
    }

}
