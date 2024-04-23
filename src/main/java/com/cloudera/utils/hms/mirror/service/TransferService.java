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

import com.cloudera.utils.hms.mirror.*;
import com.cloudera.utils.hms.mirror.connections.ConnectionException;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategy;
import com.cloudera.utils.hms.mirror.datastrategy.DataStrategyEnum;
import com.cloudera.utils.hms.mirror.datastrategy.HybridAcidDowngradeInPlaceDataStrategy;
import com.cloudera.utils.hms.mirror.datastrategy.HybridDataStrategy;
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
    private HmsMirrorCfgService hmsMirrorCfgService;
    private TableService tableService;
    private DatabaseService databaseService;
    private DataStrategyService dataStrategyService;
    private HybridDataStrategy hybridDataStrategy;
    private HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy;

    @Autowired
    public void setHmsMirrorCfgService(HmsMirrorCfgService hmsMirrorCfgService) {
        this.hmsMirrorCfgService = hmsMirrorCfgService;
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
        HmsMirrorConfig hmsMirrorConfig = getHmsMirrorCfgService().getHmsMirrorConfig();

        try {
            Date start = new Date();
            log.info("Migrating {}.{}", tableMirror.getParent().getName(), tableMirror.getName());

            EnvironmentTable let = tableMirror.getEnvironmentTable(Environment.LEFT);
            EnvironmentTable tet = tableMirror.getEnvironmentTable(Environment.TRANSFER);
            EnvironmentTable set = tableMirror.getEnvironmentTable(Environment.SHADOW);
            EnvironmentTable ret = tableMirror.getEnvironmentTable(Environment.RIGHT);

            // Set Database to Transfer DB.
            tableMirror.setPhaseState(PhaseState.STARTED);

            tableMirror.setStrategy(hmsMirrorConfig.getDataStrategy());
//            tblMirror.setResolvedDbName(config.getResolvedDB(tblMirror.getParent().getName()));

            tableMirror.incPhase();
            tableMirror.addStep("TRANSFER", hmsMirrorConfig.getDataStrategy().toString());
            try {
                DataStrategy dataStrategy = null;
                switch (hmsMirrorConfig.getDataStrategy()) {
                    case HYBRID:
                        if (TableUtils.isACID(let) && hmsMirrorConfig.getMigrateACID().isDowngradeInPlace()) {
                            if (hybridAcidDowngradeInPlaceDataStrategy.execute(tableMirror)) {
                                rtn.setStatus(ReturnStatus.Status.SUCCESS);
                            } else {
                                rtn.setStatus(ReturnStatus.Status.ERROR);
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
                        dataStrategy = getDataStrategyService().getDefaultDataStrategy(hmsMirrorConfig);
                        if (dataStrategy.execute(tableMirror)) {
                            rtn.setStatus(ReturnStatus.Status.SUCCESS);
                        } else {
                            rtn.setStatus(ReturnStatus.Status.ERROR);
                        }
                        break;
                }
                // Build out DISTCP workplans.
                if (rtn.getStatus() == ReturnStatus.Status.SUCCESS && hmsMirrorConfig.getTransfer().getStorageMigration().isDistcp()) {
                    // Build distcp reports.
                    if (hmsMirrorConfig.getTransfer().getIntermediateStorage() != null) {
                        // LEFT PUSH INTERMEDIATE
                        // The Transfer Table should be available.
                        String isLoc = hmsMirrorConfig.getTransfer().getIntermediateStorage();
                        // Deal with extra '/'
                        isLoc = isLoc.endsWith("/") ? isLoc.substring(0, isLoc.length() - 1) : isLoc;
                        isLoc = isLoc + "/" +
                                hmsMirrorConfig.getTransfer().getRemoteWorkingDirectory() + "/" +
                                hmsMirrorConfig.getRunMarker() + "/" +
                                tableMirror.getParent().getName() + ".db/" +
                                tableMirror.getName();

                        hmsMirrorConfig.getTranslator().addLocation(tableMirror.getParent().getName(), Environment.LEFT,
                                TableUtils.getLocation(tableMirror.getName(), let.getDefinition()),
                                isLoc, 1);
                        // RIGHT PULL from INTERMEDIATE
                        String fnlLoc = null;
                        if (!set.getDefinition().isEmpty()) {
                            fnlLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                        } else {
                            fnlLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (fnlLoc == null && hmsMirrorConfig.isResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(hmsMirrorConfig.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName())).append(".db").append("/").append(tableMirror.getName());
                                fnlLoc = sbDir.toString();
                            }
                        }
                        hmsMirrorConfig.getTranslator().addLocation(tableMirror.getParent().getName(), Environment.RIGHT,
                                isLoc,
                                fnlLoc, 1);
                    } else if (hmsMirrorConfig.getTransfer().getCommonStorage() != null && hmsMirrorConfig.getDataStrategy() != DataStrategyEnum.STORAGE_MIGRATION) {
                        // LEFT PUSH COMMON
                        String origLoc = TableUtils.isACID(let) ?
                                TableUtils.getLocation(let.getName(), tet.getDefinition()) :
                                TableUtils.getLocation(let.getName(), let.getDefinition());
                        String newLoc = null;
                        if (TableUtils.isACID(let)) {
                            if (hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                                newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                            } else {
                                newLoc = TableUtils.getLocation(ret.getName(), set.getDefinition());
                            }
                        } else {
                            newLoc = TableUtils.getLocation(ret.getName(), ret.getDefinition());
                        }
                        if (newLoc == null && hmsMirrorConfig.isResetToDefaultLocation()) {
                            String sbDir = hmsMirrorConfig.getTransfer().getCommonStorage() +
                                    hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory() + "/" +
                                    getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName()) + ".db" + "/" + tableMirror.getName();
                            newLoc = sbDir;
                        }
                        hmsMirrorConfig.getTranslator().addLocation(tableMirror.getParent().getName(), Environment.LEFT,
                                origLoc, newLoc, 1);
                    } else {
                        // RIGHT PULL
                        if (TableUtils.isACID(let)
                                && !hmsMirrorConfig.getMigrateACID().isDowngrade()
                                && !(hmsMirrorConfig.getDataStrategy() == DataStrategyEnum.STORAGE_MIGRATION)) {
                            tableMirror.addIssue(Environment.RIGHT, DISTCP_FOR_SO_ACID.getDesc());
                            rtn.setStatus(ReturnStatus.Status.ERROR);//successful = Boolean.FALSE;
                        } else if (TableUtils.isACID(let) && hmsMirrorConfig.getMigrateACID().isDowngrade()) {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (rLoc == null && hmsMirrorConfig.isResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(hmsMirrorConfig.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName())).append(".db").append("/").append(tableMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            hmsMirrorConfig.getTranslator().addLocation(tableMirror.getParent().getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), tet.getDefinition()),
                                    rLoc, 1);
                        } else {
                            String rLoc = TableUtils.getLocation(tableMirror.getName(), ret.getDefinition());
                            if (rLoc == null && hmsMirrorConfig.isResetToDefaultLocation()) {
                                StringBuilder sbDir = new StringBuilder();
                                if (hmsMirrorConfig.getTransfer().getCommonStorage() != null) {
                                    sbDir.append(hmsMirrorConfig.getTransfer().getCommonStorage());
                                } else {
                                    sbDir.append(hmsMirrorConfig.getCluster(Environment.RIGHT).getHcfsNamespace());
                                }
                                sbDir.append(hmsMirrorConfig.getTransfer().getWarehouse().getExternalDirectory()).append("/");
                                sbDir.append(getHmsMirrorCfgService().getResolvedDB(tableMirror.getParent().getName())).append(".db").append("/").append(tableMirror.getName());
                                rLoc = sbDir.toString();
                            }
                            hmsMirrorConfig.getTranslator().addLocation(tableMirror.getParent().getName(), Environment.RIGHT,
                                    TableUtils.getLocation(tableMirror.getName(), let.getDefinition())
                                    , rLoc, 1);
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
        } catch (Throwable t) {
            rtn.setStatus(ReturnStatus.Status.FATAL);
            rtn.setException(t);
        }
        return new AsyncResult<>(rtn);
    }

}
