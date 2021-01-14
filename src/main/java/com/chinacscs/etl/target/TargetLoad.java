package com.chinacscs.etl.target;

import com.chinacscs.etl.alert.AlertFactory;
import com.chinacscs.etl.alert.IAlert;
import com.chinacscs.jdbc.JdbcManager;
import com.chinacscs.jdbc.ListResultHandler;
import com.chinacscs.params.Constants;
import com.chinacscs.utils.*;
import com.zaxxer.hikari.pool.HikariPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author wangjj
 * @date 2017/1/3 20:38
 * @description 将数据同步到应用数据库
 * @copyright(c) chinacscs all rights reserved
 */
public class TargetLoad {
    private static final Logger logger = LoggerFactory.getLogger(TargetLoad.class);
    String sourceCode;
    JdbcManager jdbcManagerMds;
    JdbcManager jdbcManagerStg;
    JdbcManager jdbcManagerTgt;
    JdbcManager jdbcManagerStgMonitor;
    JdbcManager jdbcManagerTgtMonitor;
    ExecutorService pool;
    List<List<String>> targetTableList;
    List<String> errorTableList = new ArrayList<>();
    String logPath = System.getProperty("log.dir") + "/TargetLoad.log";
    String sourceName;
    String databaseType;
    Integer clientId;
    Integer subscribeId;
    IAlert alert;
    Boolean useTempTable;
    Integer batchSize;
    /** MDS分发表视图名称 */
    private String subscribeView;
    /** Target入库时分页大小 */
    private Integer pageSize;
    /** 数据临时表 */
    private String dataTempTable;
    /** 同步类型 */
    private Integer syncType;

    private String dburlStg;


    public TargetLoad(String sourceCode) {
        this.sourceCode = sourceCode;
        String driverMds = PropUtils.getString("mds.jdbc.driver");
        String dburlMds = PropUtils.getString("mds.jdbc.url");
        String userMds = PropUtils.getString("mds.jdbc.user");
        String passwordMds = PropUtils.getString("mds.jdbc.password");
        String driverStg = PropUtils.getString("stage.jdbc.driver");
        dburlStg = PropUtils.getString("stage.jdbc.url");
        String userStg = PropUtils.getString("stage.jdbc.user");
        String passwordStg = PropUtils.getString("stage.jdbc.password");
        String driverTgt = PropUtils.getString("target.jdbc.driver");
        String dburlTgt = PropUtils.getString("target.jdbc.url");
        String userTgt = PropUtils.getString("target.jdbc.user");
        String passwordTgt = PropUtils.getString("target.jdbc.password");
        databaseType = dburlStg.split(":")[1];
        if (!Constants.SUPPORTED_DATABASE.contains(databaseType.toLowerCase())) {
            logger.error("不支持的数据库:" + databaseType);
            System.exit(-1);
        }
        if (sourceCode.equals("csdc")) {
            sourceName = "CISP";
        } else {
            sourceName = sourceCode.toUpperCase();
        }
        try {
            try {
                alert = AlertFactory.getAlert();
            } catch (Exception e) {
                logger.error("初始化Alert实例失败！");
                System.exit(-1);
            }
            try {
                this.jdbcManagerMds = new JdbcManager(dburlMds, userMds, passwordMds, driverMds);
            } catch (HikariPool.PoolInitializationException e) {
                logger.error("MDS数据库连接建立失败！");
                LogUtils.logException(logger, e);
                //发送预警
                String subject = "[S" + subscribeId + "C" + clientId + "]MDS数据库连接失败！";
                String message = "请查看附件中的日志获取详细信息。。";
                sendAlert(subject, message, Arrays.asList(logPath));
                System.exit(-1);
            }
            try {
                this.jdbcManagerStg = new JdbcManager(dburlStg, userStg, passwordStg, driverStg);
                this.jdbcManagerStgMonitor = new JdbcManager(dburlStg, userStg, passwordStg, driverStg);

            } catch (HikariPool.PoolInitializationException e) {
                logger.error("Staging数据库连接建立失败！");
                LogUtils.logException(logger, e);
                //发送预警
                String subject = "[S" + subscribeId + "C" + clientId + "]staging数据库连接失败！";
                String message = "请查看附件中的日志获取详细信息。。";
                sendAlert(subject, message, Arrays.asList(logPath));
                System.exit(-1);
            }
            try {
                this.jdbcManagerTgt = new JdbcManager(dburlTgt, userTgt, passwordTgt, driverTgt);
                this.jdbcManagerTgtMonitor = new JdbcManager(dburlTgt, userTgt, passwordTgt, driverTgt);
            } catch (HikariPool.PoolInitializationException e) {
                logger.error("target数据库连接建立失败！");
                LogUtils.logException(logger, e);
                //发送预警
                String subject = "[S" + subscribeId + "C" + clientId + "]应用数据库连接失败！";
                String message = "请查看附件中的日志获取详细信息。。";
                sendAlert(subject, message, Arrays.asList(logPath));
                System.exit(-1);
            }

            String clientName = PropUtils.getString("login.username");
            String clientPassword = PropUtils.getString("login.password");
            String loginSql = "select client_id, subscribe_id from VW_CLIENT_BASICINFO_" + clientName.toUpperCase() + " where client_snm = ? and client_pw = md5(md5(CONCAT(?,?))) and isdel = 0";
            Object[] loginParams = new Object[]{
                    clientName,
                    clientPassword,
                    clientPassword
            };
            List<List<Object>> info = (List<List<Object>>) jdbcManagerMds.executeQuery(loginSql, new ListResultHandler(), loginParams, true);
            if (info.size() < 1) {
                throw new RuntimeException("login.passowrd不正确！");
            }
            clientId = Integer.valueOf(info.get(0).get(0).toString());
            subscribeId = Integer.valueOf(info.get(0).get(1).toString());

            Integer defaultPoolSize = Runtime.getRuntime().availableProcessors() * 2 + 1;
            Integer poolSize = PropUtils.getInteger("target.threads");
            pool = poolSize == null ? Executors.newFixedThreadPool(defaultPoolSize) : Executors.newFixedThreadPool(poolSize);
            //是否使用数据库临时表
            useTempTable = PropUtils.getBoolean("target.useTempTable");
            //批量执行时分批执行的size
            batchSize = PropUtils.getInteger("jdbc.batch.size") != null ? PropUtils.getInteger("jdbc.batch.size") : 10000;
            logger.info("是否使用数据库临时表:" + useTempTable + " 批量执行时分批大小： " + batchSize);
        } catch (ClassNotFoundException e) {
            logger.error("找不到JDBC驱动！");
            LogUtils.logException(logger, e);
            System.exit(1);
        } catch (SQLException e) {
            logger.error("登录失败！请检查mds.jdbc.password参数及login.passowrd参数！");
            LogUtils.logException(logger, e);
            System.exit(1);
        }

        //根据导出环境信息获取MDS分发视图
        try {
            String exportEnv = PropUtils.getString("export.env");
            //如果没有特别指定导出环境，则使用默认的分发视图
            if (StringUtils.isEmpty(exportEnv)) {
                subscribeView = Constants.DEFAULT_SUBSCRIBE_VIEW.toUpperCase();
            } else {
                String getEnvSql = "select subscribe_view from ENVIRONMENT where isdel = 0 and enviroment_nm = ? ";
                List<List<Object>> envList = (List<List<Object>>) jdbcManagerMds.executeQuery(getEnvSql, new ListResultHandler(), new Object[]{exportEnv}, false);
                if (envList.isEmpty()) {
                    logger.info("指定的导出环境：{} 错误,无对应的导出环境配置!", exportEnv);
                    String alertMessage = "指定的导出环境：" + exportEnv + " 错误,无对应的导出环境配置!";
                    sendAlert("[S" + subscribeId + "C" + clientId + "]导出环境配置错误", alertMessage, Arrays.asList(logPath));
                    System.exit(-1);
                }
                subscribeView = envList.get(0).get(0).toString().trim().toUpperCase();
            }
            logger.info("导出环境：{}, 对应的分发视图:{}", exportEnv, subscribeView);
        } catch (SQLException e) {
            String errorMsg = "获取导出环境配置信息时出错!";
            logger.error(errorMsg);
            LogUtils.logException(logger, e);
            sendAlert("[S" + subscribeId + "C" + clientId + "]导出环境配置错误", errorMsg, Arrays.asList(logPath));
            System.exit(-1);
        }
        pageSize = PropUtils.getOrDefault("target.pageSize", -1);
        logger.debug("Target加载分页配置: {}", pageSize);
        syncType = PropUtils.getOrDefault("sync.type", Constants.SYNC_TYPE_MDS);
    }

    private int start() {
        int status = 0;
        final boolean[] hasError = {false};
        //记录本批次开始的时间
        Date batchStartDate = new Date();



        try {
            //获取每个待加载的targetTable
            String getTargetTableSql = "SELECT STG_TABLE, STG_FIELD_LIST, TGT_TABLE, TGT_FIELD_LIST, TGT_LOGIC_PK1, TGT_LOGIC_PK2, TGT_PHYSICAL_PK, PROCESS_TYPE, IF_PRIOR FROM " + subscribeView + " WHERE ISDEL = 0 AND TGT_TABLE!='0' AND (CLIENT_ID = ? OR CLIENT_ID IS NULL) AND SUBSCRIBE_ID = ? " +
                    "ORDER BY CASE " +
                    "WHEN TGT_TABLE='COMPY_FINANCE' THEN 1 " +
                    "WHEN TGT_TABLE='COMPY_FINANCE_LAST_Y' THEN 2 " +
                    "WHEN TGT_TABLE='COMPY_FINANCE_BF_LAST_Y' THEN 3 " +
                    "WHEN TGT_TABLE='COMPY_FACTOR_FINANCE' THEN 5 " +
                    "WHEN TGT_TABLE='COMPY_FACTOR_OPERATION' THEN 6 ELSE 4 END";
            logger.debug(getTargetTableSql);
            targetTableList = ((List<List<Object>>) jdbcManagerMds.executeQuery(getTargetTableSql, new ListResultHandler(), new Object[]{clientId, subscribeId}, true))
                    .stream()
                    .map(list -> list.stream()
                            .map(obj -> obj == null ? null : obj.toString())
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());
            logger.info("即将进行数据同步的target表数量：{}", targetTableList.size());

            //查出当前的BATCHSID
            String getBatchSidSql = null;
            if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                getBatchSidSql = "select nvl(max(loadbatch_sid), 0) from etl_stg_run_details a where a.run_details_sid = (select max(run_details_sid) from etl_stg_run_details)";
            } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                getBatchSidSql = "select coalesce(max(loadbatch_sid), 0) from etl_stg_run_details a where a.run_details_sid =(select max(run_details_sid) from etl_stg_run_details)";
            } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                getBatchSidSql = "select ifnull(max(loadbatch_sid), 0) from etl_stg_run_details a where a.run_details_sid =(select max(run_details_sid) from etl_stg_run_details)";
            }
            Long BatchSid = -1L;
            BatchSid = (Long) jdbcManagerStgMonitor.executeQuery(getBatchSidSql, rs -> {
                rs.next();
                return rs.getLong(1);
            }, null, false);
            final Long FinalBatchSid=BatchSid;
            String MonitorlogInsertSql="insert into etl_tgt_run_details(run_details_sid,process_nm,loadbatch_sid,src_table,dest_table,orig_record_count,begin_dt,run_status,is_del,insert_dt,updt_dt) values(?,'MasterDataSync',?,?,?,?,?,0,0,?,?)";
            String MointorlogUpdateSql="update etl_tgt_run_details set orig_record_count=?,dup_record_count=?,insert_count=?,updt_count=?,end_dt=?,run_status=1,updt_dt=? where run_details_sid=?";
            String MointorlogUpdateErrorSql="update etl_tgt_run_details set err_message=?,run_status=-1,updt_dt=? where run_details_sid=?";

            //遍历每个targetTable，每个targetTable起一个线程进行加载
            targetTableList.forEach(targetTableInfo -> {
                pool.execute(() -> {
                    String targetTable = null;
                    Long MonitorlogSid = -1L;

                    try {
                        String stageTable = targetTableInfo.get(0).toUpperCase().trim();
                        List<String> stageFieldList = Arrays.stream(targetTableInfo.get(1).split(","))
                                .map(String::trim)
                                .map(String::toLowerCase)
                                .filter(fieldName -> !fieldName.equals("record_sid") && !fieldName.equals("loadlog_sid"))
                                .collect(Collectors.toList());
                        String stageFieldStr = String.join(",", stageFieldList);
                        targetTable = targetTableInfo.get(2).toUpperCase().trim();
                        List<String> targetFieldList = Arrays.stream(targetTableInfo.get(3).split(","))
                                .map(String::trim)
                                .map(String::toLowerCase)
                                .collect(Collectors.toList());
                        String targetLogicPkCSCS = targetTableInfo.get(4);
                        List<String> targetLogicPkCSCSList = Arrays.stream(targetTableInfo.get(4).split(","))
                                .map(String::trim)
                                .map(String::toLowerCase)
                                .collect(Collectors.toList());
                        List<String> targetLogicPkCustomerList = Arrays.stream(targetTableInfo.get(5).split(","))
                                .map(String::trim)
                                .map(String::toLowerCase)
                                .collect(Collectors.toList());
                        String targetPhysicalPk = targetTableInfo.get(6);
                        List<String> targetPhysicalPkList = Arrays.stream(targetTableInfo.get(6).split(","))
                                .map(String::trim)
                                .map(String::toLowerCase)
                                .collect(Collectors.toList());
                        Integer processType = Integer.valueOf(targetTableInfo.get(7));
                        //是否要覆盖客户数据（数据优先级）
                        Integer ifPrior = Integer.valueOf(targetTableInfo.get(8));


                        logger.info("开始加载" + targetTable + "...处理类型:" + processType + "...数据优先级:" + ifPrior);

                        String getStageDataCountSql = "select count(*) from " + stageTable;
                        logger.debug(getStageDataCountSql);
                        Long stageDataCount = (Long) jdbcManagerStg.executeQuery(getStageDataCountSql, rs -> {
                            rs.next();
                            return rs.getLong(1);
                        }, null, false);
                        logger.info(stageTable + "表数据总数: {}", stageDataCount);
                        //stage表记录为空，直接返回
                        if (stageDataCount == 0) {
                            logger.warn(stageTable + "表中记录为空，跳过...");
                            return;
                        }

                        //查出当前的MONITORLOGSID
                        String getMonitorLogSidSql = null;
                        if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                            getMonitorLogSidSql = "SELECT seq_etl_tgt_run_details.nextval from dual";
                        } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                            getMonitorLogSidSql = "SELECT NEXTVAL('seq_etl_tgt_run_details')";
                        } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                            //获取STG连接库的Schema名称
                            String stgSchema = dburlStg.substring(dburlStg.lastIndexOf("/") + 1);
                            getMonitorLogSidSql = "SELECT auto_increment FROM information_schema.`TABLES` WHERE TABLE_SCHEMA='" + stgSchema + "' AND TABLE_NAME='ETL_TGT_RUN_DETAILS'";
                        }

                        MonitorlogSid = (Long) jdbcManagerTgtMonitor.executeQuery(getMonitorLogSidSql, rs -> {
                            rs.next();
                            return rs.getLong(1);
                        }, null, false);

                        Timestamp MonitorBeginDate = new Timestamp(System.currentTimeMillis());
                        Object[] MonitorlogInsertParams = new Object[]{
                                MonitorlogSid,
                                FinalBatchSid,
                                stageTable,
                                targetTable,
                                stageDataCount,
                                MonitorBeginDate,
                                MonitorBeginDate,
                                MonitorBeginDate
                        };
                        jdbcManagerTgtMonitor.executeUpdate(MonitorlogInsertSql, MonitorlogInsertParams, false, false);
                        jdbcManagerTgtMonitor.commit();

                        //去重时排序字段
                        String distinctOrderColumn;
                        if (stageFieldList.contains("updt_ts")) {
                            distinctOrderColumn = "updt_ts";
                        } else {
                            distinctOrderColumn = "updt_dt";
                        }
                        //去重时归类字段, 默认为targetLogicPkCSCS, 特殊表中使用targetPhysicalPk
                        String rankColumn = targetTable.equalsIgnoreCase("compy_industry") ? targetPhysicalPk : targetLogicPkCSCS;
                        //数据通用查询部分
                        String commonFromSql = " FROM (select " + stageFieldStr + ",row_number() over(partition by " + rankColumn + " order by " + distinctOrderColumn + " desc ) rn FROM " + stageTable + ") a where rn = 1";
                        //Mysql数据库中对Stage表去重SQL做特殊处理
                        if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                            commonFromSql = " FROM " + stageTable
                                    + " A join ( select substring_index(group_concat(record_sid order by " + distinctOrderColumn + " desc),',', 1) as record_sid from " + stageTable + " group by " + rankColumn + ") B ON A.record_sid = B.record_sid ";
                        }
                        String getStageTableDataSql = "SELECT " + stageFieldStr + commonFromSql;

                        //如果stage数据中的updt_dt字段存在，则设为当前时间
                        int updateDateIndex = targetFieldList.indexOf("updt_dt");
                        //债券存续期项目中使用的是updt_ts而不是updt_dt
                        if (updateDateIndex < 0) {
                            updateDateIndex = targetFieldList.indexOf("updt_ts");
                        }
                        //记录开始加载的时间
                        Timestamp startDate = new Timestamp(System.currentTimeMillis());


                        //数据临时表名称
                        if (targetTable.length()>26) {

                            dataTempTable = ("tmp_" + targetTable.substring(4)).toUpperCase();
                        } else {

                            dataTempTable = ("tmp_" + targetTable).toUpperCase();

                        }
                        //在target创建用于存放stage表数据的临时表
                        createTempTableForStage(dataTempTable, targetTable);

                        Integer loadCount;
                        //判断是否要进行分页处理
                        if (pageSize != null && pageSize > 0) {
                            String countSql = "SELECT COUNT(*) " + commonFromSql;
                            loadCount = (Integer) jdbcManagerStg.executeQuery(countSql, rs -> {
                                rs.next();
                                return rs.getInt(1);
                            }, null, false);
                            logger.info("待同步的数据总数：{}", loadCount);
                            logger.info("开始插入" + targetTable + "变化的记录....");
                            //计算分页的数量,开始循环分页导出
                            Integer totalPage = (int) Math.ceil((double) loadCount / pageSize);
                            logger.debug("总共分：{} 页进行加载", totalPage);
                            for (int i = 0; i < totalPage; i++) {
                                logger.debug("开始加载第 {} 页数据", i + 1);
                                int start = i * pageSize;
                                //PostgreSQL分页语法
                                String pageLoadSql = getStageTableDataSql + " limit " + pageSize + " offset " + start;
                                if (Constants.DB_TYPE_MYSQL.equalsIgnoreCase(databaseType)) {
                                    //Mysql分页语法
                                    pageLoadSql = getStageTableDataSql + " limit " + start + "," + pageSize;
                                } else if (Constants.DB_TYPE_ORACLE.equalsIgnoreCase(databaseType)) {
                                    //Oracle分页语法
                                    int end = start + pageSize;
                                    pageLoadSql = "SELECT " + stageFieldStr + " FROM (SELECT a.*, ROWNUM as rowno " + commonFromSql + " AND ROWNUM <= " + end + ") WHERE rowno > " + start;
                                }
                                //将数据插入临时表
                                insertDataToTempTable(dataTempTable, pageLoadSql, targetFieldList, updateDateIndex);
                            }
                        } else {
                            //不需要分页，直接将全部数据插入临时表
                            logger.info("开始插入" + targetTable + "变化的记录....");
                            //将数据插入临时表
                            loadCount = insertDataToTempTable(dataTempTable, getStageTableDataSql, targetFieldList, updateDateIndex);
                        }
                        logger.info("Stage表:{}中数据已全部同步到Target临时表:{}中", stageTable, dataTempTable);
                        //开始将数据加载到Target表,并记录加载结果
                        Map<String, Integer> resultMap;
                        //根据target表类型的不同，用不同的方式加载target表
                        switch (processType) {
                            case 1: {
                                resultMap = targetLoadType1(targetTable, targetPhysicalPkList);
                                break;
                            }
                            case 2: {
                                resultMap = targetLoadType2(targetTable, targetFieldList, targetPhysicalPkList, targetLogicPkCSCSList);
                                break;
                            }
                            case 3: {
                                resultMap = targetLoadType3(stageTable, targetTable, targetFieldList, targetPhysicalPkList, targetLogicPkCustomerList, ifPrior);
                                break;
                            }
                            case 4: {
                                resultMap = targetLoadType4(stageTable, targetTable, targetFieldList, targetPhysicalPkList, targetLogicPkCSCSList, ifPrior);
                                break;
                            }
                            case 5: {
                                resultMap = targetLoadType5(stageTable, targetTable, targetFieldList, targetPhysicalPkList, ifPrior);
                                break;
                            }
                            default: {
                                logger.error(targetTable + "的处理类型不正确！");
                                //删除临时表
                                dropTempTable(dataTempTable);
                                return;
                            }
                        }
                        //删除临时表
                        dropTempTable(dataTempTable);
                        logger.info("新增数据已全部从临时表:{} 同步到 {} 表中", dataTempTable, targetTable);

                        //记录结束加载时间
                        Timestamp endDate = new Timestamp(System.currentTimeMillis());
                        Long duplicateDataCount = stageDataCount - loadCount;
                        if (syncType == Constants.SYNC_TYPE_CLIENT) {
                            // 在客户数据输入时兼容当前生产环境中的日志表结构
                            localLog(targetTable, stageDataCount, duplicateDataCount, resultMap.get("insertCount"), resultMap.get("updateCount"), startDate, endDate);
                        } else {
                            //获取加载记录的起始和结束id
                            String getRowidSql = "SELECT MIN(RECORD_SID), MAX(RECORD_SID) FROM " + stageTable;
                            List<List<Object>> rowidResult = (List<List<Object>>) jdbcManagerStg.executeQuery(getRowidSql, new ListResultHandler(), null, false);
                            Long startRowid = Long.parseLong(rowidResult.get(0).get(0).toString());
                            Long endRowid = Long.parseLong(rowidResult.get(0).get(1).toString());
                            logger.debug("读取了" + stageTable + "表中ID范围" + startRowid + "到" + endRowid + "的记录");
                            //记录日志
                            log(targetTable, stageDataCount, duplicateDataCount, resultMap.get("insertCount"), resultMap.get("updateCount"), startDate, endDate, startRowid, endRowid);
                        }

                        //target表加载完毕后将stage表的记录移入hist表, 并清空stage表
                        archiveStageTableData(stageTable);

                        //提交并关闭连接
                        jdbcManagerStg.commit();
                        jdbcManagerTgt.commit();
                        logger.info(targetTable + "表加载完毕");

                        Timestamp MonitorEndDate = new Timestamp(System.currentTimeMillis());
                        Object[] MonitorlogUpdateParams = new Object[]{
                                stageDataCount,
                                duplicateDataCount,
                                resultMap.get("insertCount"),
                                resultMap.get("updateCount"),
                                MonitorEndDate,
                                MonitorEndDate,
                                MonitorlogSid
                        };
                        jdbcManagerTgtMonitor.executeUpdate(MointorlogUpdateSql, MonitorlogUpdateParams, false, false);
                        jdbcManagerTgtMonitor.commit();

                    } catch (Exception e) {
                        hasError[0] = true;
                        errorTableList.add(targetTable);
                        logger.error(targetTable + "表加载失败");
                        LogUtils.logException(logger, e);
                        try {
                            jdbcManagerStg.rollback();
                            jdbcManagerTgt.rollback();
                            Timestamp MonitorErrorEndDate = new Timestamp(System.currentTimeMillis());
                            Object[] MonitorErrorlogUpdateParams = new Object[]{
                                    e.toString(),
                                    MonitorErrorEndDate,
                                    MonitorlogSid
                            };
                            jdbcManagerTgtMonitor.executeUpdate(MointorlogUpdateErrorSql, MonitorErrorlogUpdateParams, false, false);
                            jdbcManagerTgtMonitor.commit();
                        } catch (SQLException e1) {
                            LogUtils.logException(logger, e);
                        }
                    } finally {
                        try {
                            jdbcManagerMds.close();
                            jdbcManagerStg.close();
                            jdbcManagerTgt.close();
                            jdbcManagerStgMonitor.close();
                            jdbcManagerTgtMonitor.close();
                        } catch (SQLException e) {
                            LogUtils.logException(logger, e);
                        }
                    }
                });
            });
            pool.shutdown();
            while (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
            }

            //在GDS同步方式下,兼容数据删除处理
            Integer syncType = PropUtils.getOrDefault("sync.type", Constants.SYNC_TYPE_MDS);
            Boolean handleDelete = PropUtils.getBoolean("target.handleDelete");
            if (syncType.equals(Constants.SYNC_TYPE_GDS) && handleDelete) {
                //根据删除记录表进行Target中对应表数据的删除
                try {
                    deleteRecordFromTable(subscribeId, clientId);
                } catch (Exception e) {
                    hasError[0] = true;
                    logger.error("根据删除记录表删除对应表中数据时出错:{}", e.getMessage());
                    LogUtils.logException(logger, e);
                }
            }

            //生成报告文件
            String reportPath = PropUtils.getString("stage." + sourceCode + ".baseDir") + "/TargetLoadReport_" + DateUtils.toString(batchStartDate, "yyyyMMddHHmmss") + ".csv";
            generateSyncReport(reportPath, batchStartDate);

            //发送报警
            try {
                String logPath = System.getProperty("log.dir") + "/TargetLoad.log";
                String subject;
                String message = "开始时间：" + DateUtils.toString(batchStartDate, "yyyy-MM-dd HH:mm:ss") + "\n结束时间：" + DateUtils.getCurrentTimePretty();
                if (hasError[0]) {
                    subject = "[S" + subscribeId + "C" + clientId + "]向应用数据库中加载数据的过程中发生了错误！";
                    logger.error(subject);
                    message = message + "\n以下表加载失败，请查看日志获取详细信息";
                    for (String errorTable : errorTableList) {
                        message += "\n" + errorTable;
                    }
                    sendAlert(subject, message, Arrays.asList(logPath, reportPath));
                } else {
                    subject = "[S" + subscribeId + "C" + clientId + "]数据已加载至应用数据库";
                    logger.info(subject);
                    sendAlert(subject, message, Arrays.asList(logPath, reportPath));
                }
            } catch (Exception e) {
                logger.error("报警发送失败！");
                LogUtils.logException(logger, e);
            }
        } catch (Exception e) {
            status = 2;
            LogUtils.logException(logger, e);
            //发送报警
            String subject = "[S" + subscribeId + "C" + clientId + "]向应用数据库中加载数据的过程中发生了错误！";
            String message = "请查看附件中的日志获取详细信息。。";
            sendAlert(subject, message, Arrays.asList(logPath));
        } finally {
            try {
                pool.shutdown();
                jdbcManagerStg.close();
                jdbcManagerTgt.close();
            } catch (SQLException e) {
                LogUtils.logException(logger, e);
            }
        }
        return status;
    }

    /**
     * 创建数据临时表,将stage表的数据作为一个临时表存放在target库中
     * 如果启用了真正的临时表处理，则不用创建物理临时表
     *
     * @param tempTable
     * @param targetTable
     * @throws SQLException
     */
    public void createTempTableForStage(String tempTable, String targetTable) throws SQLException {
        //如果启用真正的临时表处理，则不用创建物理临时表
        if (!useTempTable) {
            //创建前先保证临时表不存在
            try {
                String dropTempTargetTableSql = "DROP TABLE " + tempTable;
                jdbcManagerTgt.executeUpdate(dropTempTargetTableSql, null, false, false);
            } catch (SQLException e) {
                //不做任何处理,默认临时表已经被删除
            }
            String createTempTargetTableSql = "CREATE TABLE " + tempTable + " AS SELECT * FROM " + targetTable + " WHERE 1=2";
            if (Constants.DB_TYPE_MYSQL.equalsIgnoreCase(databaseType)) {
                createTempTargetTableSql = "CREATE TABLE " + tempTable + " LIKE " + targetTable;
            }
            logger.debug("创建数据临时表:{}", createTempTargetTableSql);
            jdbcManagerTgt.executeUpdate(createTempTargetTableSql, null, false, false);
        }
    }

    /**
     * 将Stage表数据插入到Target临时表中
     *
     * @param tempTable
     * @param getStageTableDataSql
     * @param targetFieldList
     * @param updateDateIndex
     * @return
     * @throws SQLException
     */
    private Integer insertDataToTempTable(String tempTable, String getStageTableDataSql, List<String> targetFieldList, Integer updateDateIndex) throws SQLException {
        logger.debug(getStageTableDataSql);
        List<List<Object>> stageData = (List<List<Object>>) jdbcManagerStg.executeQuery(getStageTableDataSql, new ListResultHandler(), null, false);
        int loadCount = stageData.size();
        logger.info("本次待同步的数据总数：{}", loadCount);
        logger.info("开始更新所有数据的同步时间");
        //将数据更新时间替换为ETL更新的时间（即当前最新时间）
        if (updateDateIndex >= 0) {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            int finalUpdateDateIndex = updateDateIndex;
            stageData.forEach(row -> row.set(finalUpdateDateIndex, now));
        }
        logger.info("开始插入数据到临时表： {}", tempTable);
        //将数据插入到临时表中
        String insertTempTargetTableSql = "INSERT INTO " + tempTable + " VALUES(";
        for (int i = 0; i < targetFieldList.size(); i++) {
            insertTempTargetTableSql = insertTempTargetTableSql + "?,";
        }
        insertTempTargetTableSql = insertTempTargetTableSql.replaceAll(",$", ")");
        List<Object[]> stageDataParams = stageData.stream()
                .map(row -> row.stream()
                        .map(data -> data != null ? data : null)
                        .toArray())
                .collect(Collectors.toList());
        logger.debug(insertTempTargetTableSql);
        // 默认此时不提交事务
        boolean autoCommit = false;
        if (Constants.DB_TYPE_POSTGRESQL.equalsIgnoreCase(databaseType)) {
            //由于在PG环境下DDL语句是包含在事务中的，为防止删除临时表失败时导致事务回滚, PG环境中在插入数据后进行事务的提交操作
            autoCommit = true;
        }
        jdbcManagerTgt.executeBatchWithBatchSize(insertTempTargetTableSql, stageDataParams, false, autoCommit, batchSize);
        logger.info("数据已导入到临时表： {}", tempTable);
        return loadCount;
    }

    /**
     * 处理方式一：适用于业务主键就是物理主键，且客户不会维护数据的表
     * 客户端不维护数据，基于物理主键进行更新，已存在的记录则删除，然后插入所有加载数据，如代码表中的数据
     *
     * @param targetTable
     * @param targetPhysicalPkList
     */
    private Map<String, Integer> targetLoadType1(String targetTable, List<String> targetPhysicalPkList) throws SQLException, ParseException {
        //删除target表中已存在的记录
        String targetPhysicalPkStr = String.join(",", targetPhysicalPkList);
        String deleteSql = "DELETE FROM " + targetTable + " WHERE (" + targetPhysicalPkStr + ") IN (SELECT " + targetPhysicalPkStr + " FROM " + dataTempTable + ")";
        logger.debug("删除target表中已存在的记录:{}", deleteSql);
        int deleteCount = jdbcManagerTgt.executeUpdate(deleteSql, null, false, false);
        logger.info("更新了" + targetTable + "表中的" + deleteCount + "条记录");

        //插入所有加载记录
        String insertSql = "INSERT INTO " + targetTable + " SELECT * FROM " + dataTempTable;
        logger.debug(insertSql);
        int insertCount = jdbcManagerTgt.executeUpdate(insertSql, null, false, false) - deleteCount;
        logger.info("插入了" + targetTable + "表" + insertCount + "条记录");

        //返回处理结果
        Map<String, Integer> resultMap = new HashMap<>(16);
        resultMap.put("insertCount", insertCount);
        resultMap.put("updateCount", deleteCount);
        return resultMap;
    }

    /**
     * 处理方式二：适用于业务主键与物理主键不同的表，目前没有这种处理方式的表
     * 客户端不维护数据，基于业务主键进行更新，已存在的记录首先保留原来的物理主键，然后删除，并且将对应的加载记录关联到原有的物理主键，插入到客户端数据库中，对新的记录在插入之前生成新的物理主键
     *
     * @param targetTable
     * @param targetFieldList
     * @param targetPhysicalPkList
     * @param targetLogicPkCSCSList
     */
    private Map<String, Integer> targetLoadType2(String targetTable, List<String> targetFieldList, List<String> targetPhysicalPkList, List<String> targetLogicPkCSCSList) throws SQLException {
        //删除前将所有物理主键保存下来,
        String targetPhysicalPkStr = String.join(",", targetPhysicalPkList);
        String targetLogicPkCSCSStr = String.join(",", targetLogicPkCSCSList);

        //主键临时表名称
        String pkTempTable = ("tmp1_" + targetTable).toUpperCase();
        //创建主键临时表并保存相应数据
        createPkTempTable(pkTempTable, targetPhysicalPkStr, targetLogicPkCSCSStr, targetTable, null);

        //删除target表中已存在的记录
        String deleteSql = "DELETE FROM " + targetTable + " WHERE (" + targetLogicPkCSCSStr + ") IN (SELECT " + targetLogicPkCSCSStr + " FROM " + dataTempTable + ")";
        logger.debug("删除target表中已存在的记录:{}", deleteSql);
        int deleteCount = jdbcManagerTgt.executeUpdate(deleteSql, null, false, false);
        logger.info("更新了" + targetTable + "表中的" + deleteCount + "条记录");

        //插入所有加载记录
        String insertSql = "INSERT INTO " + targetTable + " SELECT ";
        for (String targetField : targetFieldList) {
            //如果是物理主键，优先取之前保存下来的物理主键
            if (targetPhysicalPkList.contains(targetField)) {
                if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                    insertSql = insertSql + "COALESCE(B." + targetField + ",SEQ_" + targetTable + ".NEXTVAL),";
                } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                    insertSql = insertSql + "COALESCE(B." + targetField + ",NEXTVAL('SEQ_" + targetTable + "')),";
                } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                    //mysql插入时自增主键的处理
                    insertSql = insertSql + "COALESCE(B." + targetField + ", null),";
                }
            } else {
                insertSql = insertSql + "A." + targetField + ",";
            }
        }
        insertSql = insertSql.replaceAll(",$", " FROM ") + dataTempTable + " A LEFT JOIN " + pkTempTable + " B ON ";
        for (String targetLogicPkCSCS : targetLogicPkCSCSList) {
            insertSql = insertSql + "A." + targetLogicPkCSCS + "=B." + targetLogicPkCSCS + " AND ";
        }
        insertSql = insertSql.replaceAll(" AND $", "");
        logger.debug(insertSql);
        int insertCount = jdbcManagerTgt.executeUpdate(insertSql, null, false, false) - deleteCount;
        logger.info("插入了" + targetTable + "表" + insertCount + "条记录");

        //删除临时表主键临时表
        dropTempTable(pkTempTable);

        //返回处理结果
        Map<String, Integer> resultMap = new HashMap<>(16);
        resultMap.put("insertCount", insertCount);
        resultMap.put("updateCount", deleteCount);
        return resultMap;
    }

    /**
     * 处理方式三：适用于需要区分用户输入和同步数据，且两种数据业务主键不同，且物理主键与业务主键不同的表，用户输入数据与同步数据采用不同区域的主键，同步数据的主键插入时不会重新生成么因为会被其他表引用，比如企业基本信息表。
     * 需要区分数据是同步数据还是用户输入的数据，并且对于同步加载的数据和用户输入的数据采用不同的业务主键（如企业基本信息表、债券基本表）.
     * 此种方式下，虽然同步加载的记录并不在客户端重新生成物理主键， 但由于客户端的物理主键与同步数据的物理主键采用分段管理，因此不会发生物理主键冲突。
     * 在数据加载时，首先基于客户的业务主键检查加载的同步数据记录是否存在于用户输入的记录中， 如果存在，则判断哪个优先级更高， 如果用户输入记录的优先级高，将对应的同步传输过来的记录丢弃掉；
     * 如果同步记录的优先级高， 则将对应的用户输入的记录isdel设为2；其次基于物理主键检查加载的同步记录是否存在于已加载的同步记录中， 如果存在，则将原有的记录删除；
     * 最后将加载的所有记录插入到客户端应用层数据库中。
     *
     * @param stageTable
     * @param targetTable
     * @param targetFieldList
     * @param targetPhysicalPkList
     * @param targetLogicPkCustomerList
     * @param ifPrior
     */
    private Map<String, Integer> targetLoadType3(String stageTable, String targetTable, List<String> targetFieldList, List<String> targetPhysicalPkList, List<String> targetLogicPkCustomerList, Integer ifPrior) throws SQLException {
        //获取Isdel字段的名字，可能是isdel，也可能是is_del
        String isdel = "isdel";
        if (targetFieldList.contains("is_del")) {
            isdel = "is_del";
        }

        //第一步，基于客户业务主键处理那些stage数据中存在于客户记录的那些数据
        String targetLogicPkCustomerStr = String.join(",", targetLogicPkCustomerList);
        int updateCount = 0;
        //判断target表中是否有src_cd，如果有则之后拿src_cd来判断是否是用户数据的依据，否则拿updt_by
        String syncDataFilter = " (updt_by = 0 or updt_by < -1) ";
        String userDataFilter = " (updt_by = -1 or updt_by > 0) ";
        if (targetFieldList.contains("src_cd")) {
            syncDataFilter = " (lower(src_cd) = '" + sourceCode + "') ";
            userDataFilter = " (lower(src_cd) != '" + sourceCode + "') ";
        }
        if (ifPrior == 1) {
            //同步记录优先级高，将target表中对应的用户输入记录isdel设为2
            String updateDateCol = null;
            if (targetFieldList.contains("updt_dt")) {
                updateDateCol = "updt_dt";
            } else if (targetFieldList.contains("updt_dt_last_y")) {
                //B表特殊处理
                updateDateCol = "updt_dt_last_y";
            } else if (targetFieldList.contains("updt_dt_bf_last_y")) {
                //C表特殊处理
                updateDateCol = "updt_dt_bf_last_y";
            } else if (targetFieldList.contains("updt_ts")) {
                //债券存续期项目中使用的是updt_ts而不是updt_dt
                updateDateCol = "updt_ts";
            }
            String setIsdel2Sql;
            Object[] params = null;
            if (updateDateCol != null) {
                setIsdel2Sql = "UPDATE " + targetTable + " SET " + updateDateCol + " = ?, " + isdel + " = 2 ";
                params = new Object[]{new Timestamp(System.currentTimeMillis())};
            } else {
                setIsdel2Sql = "UPDATE " + targetTable + " SET " + isdel + " = 2 ";
            }
            setIsdel2Sql += "WHERE " + userDataFilter + " AND (" + targetLogicPkCustomerStr + ") IN (SELECT " + targetLogicPkCustomerStr + " FROM " + dataTempTable + ") AND " + isdel + " <2";
            logger.debug(setIsdel2Sql);
            updateCount = jdbcManagerTgt.executeUpdate(setIsdel2Sql, params, false, false);
            logger.info("将" + targetTable + "表中的" + updateCount + "条用户记录的isdel设为2");
        } else {
            //客户记录优先级高，直接将stage记录丢弃
            String deleteCSCSInputSql = "DELETE FROM " + dataTempTable + " WHERE (" + targetLogicPkCustomerStr + ") IN (SELECT " + targetLogicPkCustomerStr + " FROM " + targetTable + " WHERE " + userDataFilter + " AND " + isdel + " =0)";
            logger.debug(deleteCSCSInputSql);
            int resultCount = jdbcManagerTgt.executeUpdate(deleteCSCSInputSql, null, false, false);
            logger.info("将" + stageTable + "表中的" + resultCount + "条同步记录丢弃");
        }

        //第二步，基于物理主键处理那些stage中存在于同步记录的那些数据
        //删除target表中那些已经存在的同步记录
        String targetPhysicalPkStr = String.join(",", targetPhysicalPkList);
        //这个isdel<2加不加无所谓，反正中证的记录没有isdel=2的记录
        String deleteSql = "DELETE FROM " + targetTable + " WHERE " + syncDataFilter + " AND (" + targetPhysicalPkStr + ") IN (SELECT " + targetPhysicalPkStr + " FROM " + dataTempTable + ") AND " + isdel + " <2";
        logger.debug("删除target表中已存在的记录:{}", deleteSql);
        int deleteCount = jdbcManagerTgt.executeUpdate(deleteSql, null, false, false);
        updateCount += deleteCount;
        logger.info("更新了" + targetTable + "表中的" + deleteCount + "条记录");

        //最后，将所有stage的记录插入到客户端应用数据库中
        String insertSql = "INSERT INTO " + targetTable + " SELECT * FROM " + dataTempTable;
        logger.debug(insertSql);
        int insertCount = jdbcManagerTgt.executeUpdate(insertSql, null, false, false) - deleteCount;
        logger.info("插入了" + targetTable + "表" + insertCount + "条记录");

        //返回处理结果
        Map<String, Integer> resultMap = new HashMap<>(16);
        resultMap.put("insertCount", insertCount);
        resultMap.put("updateCount", updateCount);
        return resultMap;
    }

    /**
     * 处理方式四：适用于需要区分用户输入数据和同步数据，两者业务主键相同，且两者物理主键相同。物理主键再插入时会重新生成，比如利润表。
     * 需要区分数据是同步数据还是用户输入的数据，对于所有的记录采用相同的业务主键，此种方式下，同步加载的记录需要在客户端重新生成物理主键。
     * 在数据加载时，首先基于业务主键检查加载的同步记录是否存在于用户输入的记录中，如果存在，则判断哪个优先级更高
     * 如果用户输入记录的优先级高，将对应的同步传输过来的记录丢弃掉；如果同步记录的优先级高， 则将对应的用户输入记录isdel设为2；
     * 其次基于业务主键检查同步加载的记录是否存在于已加载的的同步记录中
     * 如果存在，则将已存在记录的物理主键赋予加载记录，同时将原有的记录删除；
     * 对同步加载记录中其他记录生成新的物理主键，并将所有记录插入到客户端数据库应用层中。
     *
     * @param stageTable
     * @param targetTable
     * @param targetFieldList
     * @param targetPhysicalPkList
     * @param targetLogicPkCSCSList
     * @param ifPrior
     */
    private Map<String, Integer> targetLoadType4(String stageTable, String targetTable, List<String> targetFieldList, List<String> targetPhysicalPkList, List<String> targetLogicPkCSCSList, Integer ifPrior) throws SQLException {
        //获取Isdel字段的名字，可能是isdel，也可能是is_del
        String isdel = "isdel";
        if (targetFieldList.contains("is_del")) {
            isdel = "is_del";
        }
        //判断target表中是否有src_cd，如果有则之后拿src_cd来判断是否是用户数据的依据，否则拿updt_by
        String syncDataFilter = " (updt_by = 0 or updt_by < -1) ";
        String userDataFilter = " (updt_by = -1 or updt_by > 0) ";
        if (targetFieldList.contains("src_cd")) {
            syncDataFilter = " (lower(src_cd) = '" + sourceCode + "') ";
            userDataFilter = " (lower(src_cd) != '" + sourceCode + "') ";
        }
        //删除前将所有物理主键保存下来,
        String targetPhysicalPkStr = String.join(",", targetPhysicalPkList);
        String targetLogicPkCSCSStr = String.join(",", targetLogicPkCSCSList);
        //主键临时表名称
        String pkTempTable = ("tmp1_" + targetTable).toUpperCase();
        //创建主键临时表，并插入相应数据
        createPkTempTable(pkTempTable, targetPhysicalPkStr, targetLogicPkCSCSStr, targetTable, syncDataFilter);

        //第一步，基于业务主键处理那些stage数据中存在于用户记录的那些数据
        int updateCount = 0;
        if (ifPrior == 1) {
            //同步记录优先级高，将target表中对应的用户输入记录isdel设为2
            String updateDateCol = null;
            if (targetFieldList.contains("updt_dt")) {
                updateDateCol = "updt_dt";
            } else if (targetFieldList.contains("updt_dt_last_y")) {
                //B表特殊处理
                updateDateCol = "updt_dt_last_y";
            } else if (targetFieldList.contains("updt_dt_bf_last_y")) {
                //C表特殊处理
                updateDateCol = "updt_dt_bf_last_y";
            } else if (targetFieldList.contains("updt_ts")) {
                //债券存续期项目中使用的是updt_ts而不是updt_dt
                updateDateCol = "updt_ts";
            }
            String setIsdel2Sql;
            Object[] params = null;
            if (updateDateCol != null) {
                setIsdel2Sql = "UPDATE " + targetTable + " SET " + updateDateCol + " = ?, " + isdel + " = 2 ";
                params = new Object[]{new Timestamp(System.currentTimeMillis())};
            } else {
                setIsdel2Sql = "UPDATE " + targetTable + " SET " + isdel + " = 2 ";
            }
            setIsdel2Sql += "WHERE " + userDataFilter + " AND (" + targetLogicPkCSCSStr + ") IN (SELECT " + targetLogicPkCSCSStr + " FROM " + dataTempTable + ") AND " + isdel + " <2";
            logger.debug(setIsdel2Sql);
            updateCount = jdbcManagerTgt.executeUpdate(setIsdel2Sql, params, false, false);
            logger.info("将" + targetTable + "表中的" + updateCount + "条用户记录的isdel设为2");
        } else {
            //用户记录优先级高，直接将stage记录丢弃
            String deleteCSCSInputSql = "DELETE FROM " + dataTempTable + " WHERE (" + targetLogicPkCSCSStr + ") IN (SELECT " + targetLogicPkCSCSStr + " FROM " + targetTable + " WHERE " + userDataFilter + " AND " + isdel + " =0)";
            logger.debug(deleteCSCSInputSql);
            int resultCount = jdbcManagerTgt.executeUpdate(deleteCSCSInputSql, null, false, false);
            logger.info("将" + stageTable + "表中的" + resultCount + "条同步记录丢弃");
        }

        //第二步，基于物理主键处理那些stage中存在于同步记录的那些数据
        //删除target表中那些已经存在的同步记录
        String deleteSql = "DELETE FROM " + targetTable + " WHERE (" + targetLogicPkCSCSStr + ") IN (SELECT " + targetLogicPkCSCSStr + " FROM " + dataTempTable + ") AND  " + syncDataFilter + "  AND " + isdel + " <2";
        logger.debug("删除target表中已存在的记录:{}", deleteSql);
        int deleteCount = jdbcManagerTgt.executeUpdate(deleteSql, null, false, false);
        updateCount += deleteCount;
        logger.info("更新了" + targetTable + "表中的" + deleteCount + "条记录");

        //最后，将所有stage的记录插入到客户端应用数据库中
        String insertSql = "INSERT INTO " + targetTable + " SELECT ";
        for (String targetField : targetFieldList) {
            //如果是物理主键，优先取之前保存下来的物理主键
            if (targetPhysicalPkList.contains(targetField)) {
                if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                    insertSql = insertSql + "COALESCE(B." + targetField + ",SEQ_" + targetTable + ".NEXTVAL),";
                } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                    insertSql = insertSql + "COALESCE(B." + targetField + ",NEXTVAL('SEQ_" + targetTable + "')),";
                } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                    //mysql插入时自增主键的处理
                    insertSql = insertSql + "COALESCE(B." + targetField + ", null),";
                }
            } else {
                insertSql = insertSql + "A." + targetField + ",";
            }
        }
        insertSql = insertSql.replaceAll(",$", " FROM ") + dataTempTable + " A LEFT JOIN " + pkTempTable + " B ON ";
        for (String targetLogicPkCSCS : targetLogicPkCSCSList) {
            insertSql = insertSql + "A." + targetLogicPkCSCS + "=B." + targetLogicPkCSCS + " AND ";
        }
        insertSql = insertSql.replaceAll(" AND $", "");
        logger.debug(insertSql);
        int insertCount = jdbcManagerTgt.executeUpdate(insertSql, null, false, false) - deleteCount;
        logger.info("插入了" + targetTable + "表" + insertCount + "条记录");

        //删除临时表主键临时表
        dropTempTable(pkTempTable);

        //返回处理结果
        Map<String, Integer> resultMap = new HashMap<>(16);
        resultMap.put("insertCount", insertCount);
        resultMap.put("updateCount", updateCount);
        return resultMap;
    }

    /**
     * 处理方式五：适用于需要区分用户输入数据和同步数据，但是物理主键就是业务主键的情况，比如指标表。
     * 需要区分客户端数据是同步数据还是用户输入的数据，对于所有的记录采用物理主键判断加载记录是否存在于已加载记录中
     * 此种方式下，同步加载的记录需不要在客户端重新生成物理主键（因为物理主键就是业务主键）。
     * 在数据加载时，首先基于物理主键检查加载的同步加载记录是否存在于用户输入的记录中
     * 如果存在，则判断哪个优先级更高， 如果用户输入记录的优先级高，将对应的同步传输过来的记录丢弃掉
     * 如果同步记录的优先级高， 则将对应的用户输入记录物理删除
     * 其次物理业务主键检查加载的记录是否存在于已加载的的同步记录中
     * 如果存在，则将已存在记录删除（删除的记录需要记录到一个删除表里，因为没有物理主键，所以无法做逻辑删除）
     * 最后将所有记录插入到客户端数据库应用层中。
     *
     * @param stageTable
     * @param targetTable
     * @param targetFieldList
     * @param targetPhysicalPkList
     * @param ifPrior
     */
    private Map<String, Integer> targetLoadType5(String stageTable, String targetTable, List<String> targetFieldList, List<String> targetPhysicalPkList, Integer ifPrior) throws SQLException {
        //获取Isdel字段的名字，可能是isdel，也可能是is_del
        String isdel = "isdel";
        if (targetFieldList.contains("is_del")) {
            isdel = "is_del";
        }

        //第一步，基于业务主键处理那些stage数据中存在于客户记录的那些数据
        String targetPhysicalPkCSCSStr = String.join(",", targetPhysicalPkList);
        int updateCount = 0;
        //判断target表中是否有src_cd，如果有则之后拿src_cd来判断是否是用户数据的依据，否则拿updt_by
        String syncDataFilter;
        String userDataFilter;
        if (targetFieldList.contains("src_cd")) {
            syncDataFilter = " (lower(src_cd) = '" + sourceCode + "') ";
            userDataFilter = " (lower(src_cd) != '" + sourceCode + "') ";
        } else {
            if (targetTable.toLowerCase().equals("compy_finance_last_y")) {
                syncDataFilter = " (updt_by_last_y = 0 or updt_by_last_y < -1) ";
                userDataFilter = " (updt_by_last_y = -1 or updt_by_last_y > 0) ";
            } else if (targetTable.toLowerCase().equals("compy_finance_bf_last_y")) {
                syncDataFilter = " (updt_by_bf_last_y = 0 or updt_by_bf_last_y < -1) ";
                userDataFilter = " (updt_by_bf_last_y = -1 or updt_by_bf_last_y > 0) ";
            } else {
                syncDataFilter = " (updt_by = 0 or updt_by < -1) ";
                userDataFilter = " (updt_by = -1 or updt_by > 0) ";
            }
        }
        if (ifPrior == 1) {
            //同步记录优先级高，将target表中对应的用户输入记录物理删除
            String deleteSql = "DELETE FROM " + targetTable + " WHERE " + userDataFilter + " AND (" + targetPhysicalPkCSCSStr + ") IN (SELECT " + targetPhysicalPkCSCSStr + " FROM " + dataTempTable + ")";
            logger.debug(deleteSql);
            updateCount = jdbcManagerTgt.executeUpdate(deleteSql, null, false, false);
            logger.info("将" + targetTable + "表中的" + updateCount + "条用户记录物理删除");
        } else {
            //客户记录优先级高，直接将stage记录丢弃
            String deleteCSCSInputSql = "DELETE FROM " + dataTempTable + " WHERE (" + targetPhysicalPkCSCSStr + ") IN (SELECT " + targetPhysicalPkCSCSStr + " FROM " + targetTable + " WHERE " + userDataFilter + ")";
            logger.debug(deleteCSCSInputSql);
            int resultCount = jdbcManagerTgt.executeUpdate(deleteCSCSInputSql, null, false, false);
            logger.info("将" + stageTable + "表中的" + resultCount + "条同步记录丢弃");
        }

        //第二步，基于物理主键处理那些stage中存在于同步记录的那些数据
        //删除target表中那些已经存在的同步记录
        String targetPhysicalPkStr = String.join(",", targetPhysicalPkList);
        String deleteSql = "DELETE FROM " + targetTable + " WHERE " + syncDataFilter + " AND (" + targetPhysicalPkStr + ") IN (SELECT " + targetPhysicalPkStr + " FROM " + dataTempTable + ")";
        logger.debug("删除target表中已存在的记录:{}", deleteSql);
        int deleteCount = jdbcManagerTgt.executeUpdate(deleteSql, null, false, false);
        updateCount += deleteCount;
        logger.info("更新了" + targetTable + "表中的" + deleteCount + "条记录");

        //最后，将所有stage的记录插入到客户端应用数据库中
        String insertSql = "INSERT INTO " + targetTable + " SELECT * FROM " + dataTempTable;
        logger.debug(insertSql);
        int insertCount = jdbcManagerTgt.executeUpdate(insertSql, null, false, false) - deleteCount;
        logger.info("插入了" + targetTable + "表" + insertCount + "条记录");

        //返回处理结果
        Map<String, Integer> resultMap = new HashMap<>(16);
        resultMap.put("insertCount", insertCount);
        resultMap.put("updateCount", updateCount);
        return resultMap;
    }

    private void log(String targetTable, Long stageDataCount, Long duplicateRecordCount, int insertCount, int updateCount, Timestamp startDate, Timestamp endDate, Long startRowid, Long endRowid)
            throws SQLException {
        String logSql = null;
        if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
            logSql = "INSERT INTO ETL_DM_LOADLOG(LOADLOG_SID,PROCESS_NM,ORIG_RECORD_COUNT,DUP_RECORD_COUNT,INSERT_COUNT,UPDT_COUNT,START_DT,END_DT,START_ROWID,END_ROWID) " +
                    "VALUES(seq_etl_dm_loadlog.nextval,?,?,?,?,?,?,?,?,?)";
        } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
            logSql = "INSERT INTO ETL_DM_LOADLOG(LOADLOG_SID,PROCESS_NM,ORIG_RECORD_COUNT,DUP_RECORD_COUNT,INSERT_COUNT,UPDT_COUNT,START_DT,END_DT,START_ROWID,END_ROWID) " +
                    "VALUES(nextval('seq_etl_dm_loadlog'),?,?,?,?,?,?,?,?,?)";
        } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
            logSql = "INSERT INTO ETL_DM_LOADLOG(PROCESS_NM,ORIG_RECORD_COUNT,DUP_RECORD_COUNT,INSERT_COUNT,UPDT_COUNT,START_DT,END_DT,START_ROWID,END_ROWID) " +
                    "VALUES(?,?,?,?,?,?,?,?,?)";
        }
        Object[] params = new Object[]{
                targetTable,
                stageDataCount,
                duplicateRecordCount,
                insertCount,
                updateCount,
                startDate,
                endDate,
                startRowid,
                endRowid
        };
        jdbcManagerTgt.executeUpdate(logSql, params, false, false);
    }

    /**
     * 本地Log记录，在客户数据输入时兼容本地生产环境中log表结构
     *
     * @param targetTable
     * @param stageDataCount
     * @param duplicateRecordCount
     * @param insertCount
     * @param updateCount
     * @param startDate
     * @param endDate
     * @throws SQLException
     */
    private void localLog(String targetTable, Long stageDataCount, Long duplicateRecordCount, int insertCount, int updateCount, Timestamp startDate, Timestamp endDate)
            throws SQLException {
        //兼容当前中证生产环境中的日志表
        String logSql = null;
        if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
            logSql = "INSERT INTO ETL_DM_LOADLOG(LOADLOG_SID,PROCESS_NM,ORIG_RECORD_COUNT,DUP_RECORD_COUNT,INSERT_COUNT,UPDT_COUNT,START_DT,END_DT) " +
                    "VALUES(seq_etl_dm_loadlog.nextval,?,?,?,?,?,?,?)";
        } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
            logSql = "INSERT INTO ETL_DM_LOADLOG(LOADLOG_SID,PROCESS_NM,ORIG_RECORD_COUNT,DUP_RECORD_COUNT,INSERT_COUNT,UPDT_COUNT,START_DT,END_DT) " +
                    "VALUES(nextval('seq_etl_dm_loadlog'),?,?,?,?,?,?,?)";
        } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
            logSql = "INSERT INTO ETL_DM_LOADLOG(PROCESS_NM,ORIG_RECORD_COUNT,DUP_RECORD_COUNT,INSERT_COUNT,UPDT_COUNT,START_DT,END_DT) " +
                    "VALUES(?,?,?,?,?,?,?)";
        }
        Object[] params = new Object[]{
                targetTable,
                stageDataCount,
                duplicateRecordCount,
                insertCount,
                updateCount,
                startDate,
                endDate
        };
        jdbcManagerTgt.executeUpdate(logSql, params, false, false);
    }

    /**
     * 创建主键临时表并插入数据
     * 如果启用了真正的临时表处理，则不用创建物理临时表
     *
     * @param pkTempTableName      主键临时表名称
     * @param targetPhysicalPkStr  物理主键字段
     * @param targetLogicPkCSCSStr 逻辑主键字段
     * @param targetTable          Target表名称
     * @param syncDataFilter       过滤条件
     * @throws SQLException
     */
    private void createPkTempTable(String pkTempTableName, String targetPhysicalPkStr, String targetLogicPkCSCSStr, String targetTable, String syncDataFilter) throws SQLException {
        String filterSql = "";
        if (syncDataFilter != null && !"".equals(syncDataFilter)) {
            filterSql = " WHERE " + syncDataFilter;
        }
        //如果启用了真正的临时表处理，则不用创建物理临时表
        if (!useTempTable) {
            String createTemp1TableSql = "CREATE TABLE " + pkTempTableName + " AS SELECT " + targetPhysicalPkStr + "," + targetLogicPkCSCSStr + " FROM " + targetTable + filterSql;
            //创建前先保证临时表不存在
            try {
                String dropTempTargetTableSql = "DROP TABLE " + pkTempTableName;
                jdbcManagerTgt.executeUpdate(dropTempTargetTableSql, null, false, false);
            } catch (SQLException e) {
            }
            logger.debug(createTemp1TableSql);
            jdbcManagerTgt.executeUpdate(createTemp1TableSql, null, false, false);
            //如果是Mysql数据库，则为主键临时表建立索引
            if (databaseType.equalsIgnoreCase(Constants.DB_TYPE_MYSQL)) {
                String createIndexSql = "CREATE INDEX index_" + pkTempTableName + " ON " + pkTempTableName + "( " + targetLogicPkCSCSStr + ")";
                logger.debug(createIndexSql);
                jdbcManagerTgt.executeUpdate(createIndexSql, null, false, false);
            }
        } else {
            //启用真正的临时表时，将数据插入临时表
            String insertDataSql = "INSERT INTO　" + pkTempTableName + " SELECT " + targetPhysicalPkStr + "," + targetLogicPkCSCSStr + " FROM " + targetTable + filterSql;
            logger.debug(insertDataSql);
            jdbcManagerTgt.executeUpdate(insertDataSql, null, false, false);
        }
    }

    /**
     * 删除临时表
     * 如果启用了真正的临时表处理，则不用删除物理临时表
     *
     * @param tempTableName 临时表名称
     */
    private void dropTempTable(String tempTableName) throws SQLException {
        //如果启用了真正的临时表处理，则不用删除物理临时表
        if (!useTempTable) {
            //删除临时表
            String dropTempTargetTableSql = "DROP TABLE " + tempTableName;
            jdbcManagerTgt.executeUpdate(dropTempTargetTableSql, null, false, false);
        }
    }

    /**
     * 备份Stage表中本次加载的数据: 将数据移入Hist表，并将Stage表清空
     *
     * @param stageTable
     * @throws SQLException
     */
    private void archiveStageTableData(String stageTable) throws SQLException {
        //target表加载完毕后将stage表的记录移入hist表
        String histTable = stageTable.replaceAll("^STG", "HIST");
        String moveStageTableSql = "INSERT INTO " + histTable + " SELECT * FROM " + stageTable;
        logger.debug(moveStageTableSql);
        int insertCount = jdbcManagerStg.executeUpdate(moveStageTableSql, null, false, false);
        logger.info("将" + insertCount + "条记录从" + stageTable + "表中移入了" + histTable);

        //清空stage表
        String truncateStageTableSql = "DELETE FROM " + stageTable;
        logger.debug(truncateStageTableSql);
        int deleteCount = jdbcManagerStg.executeUpdate(truncateStageTableSql, null, false, false);
        logger.info("删除了" + stageTable + "表中的" + deleteCount + "条记录");
    }

    /**
     * 生成本次数据同步报告
     *
     * @param reportPath
     * @param batchStartDate
     * @throws Exception
     */
    private void generateSyncReport(String reportPath, Date batchStartDate) throws Exception {
        logger.info("开始生成本次stage表加载情况的统计报告...");
        String generateReportSql = "SELECT loadlog_sid,process_nm,orig_record_count,dup_record_count,insert_count,updt_count,start_dt,end_dt FROM ETL_DM_LOADLOG where start_dt >= ? and process_nm not like 'fn%'";
        logger.debug(generateReportSql);
        List<List<Object>> resultList = (List<List<Object>>) jdbcManagerTgt.executeQuery(generateReportSql, new ListResultHandler(), new Object[]{new Timestamp(batchStartDate.getTime())}, true);
        List<String[]> contents = resultList.stream()
                .map(innerList -> innerList.stream()
                        .map(obj -> obj == null ? null : obj.toString())
                        .toArray(obj -> new String[innerList.size()]))
                .collect(Collectors.toList());
        String header = "loadlog_sid,process_nm,orig_record_count,dup_record_count,insert_count,updt_count,start_dt,end_dt";
        contents.add(0, header.split(","));
        CsvUtils.writeCsv(reportPath, ',', "UTF8", contents);
    }

    /**
     * 根据删除记录从对应的表中删除数据
     *
     * @param subscribeId
     * @param clientId
     * @throws SQLException
     */
    private void deleteRecordFromTable(Integer subscribeId, Integer clientId) throws SQLException {
        logger.info("开始处理数据记录的删除");
        //获取当前客户所有的分发表对应的原始表和目标表信息
        String getAllTableSql = "SELECT SRC_TABLE, TGT_TABLE FROM " + subscribeView + " WHERE ISDEL = 0 AND CLIENT_ID = ? AND SUBSCRIBE_ID = ? ";
        logger.debug(getAllTableSql);
        List<List<Object>> allTableList = ((List<List<Object>>) jdbcManagerMds.executeQuery(getAllTableSql, new ListResultHandler(), new Object[]{clientId, subscribeId}, true));
        //获取所有的srcTable表名,需要用于IN查询,所以需要拼单引号
        List<String> tableNameList = allTableList.stream()
                .map(table -> "'" + table.get(0).toString().trim().toUpperCase() + "'")
                .collect(Collectors.toList());
        //从删除记录中筛选客户端需要删除的表
        String getDelSrcTableSql = "SELECT DISTINCT table_nm FROM TYC_DEL_REC WHERE table_nm IN (" + String.join(",", tableNameList) + ")";
        logger.debug(getDelSrcTableSql);
        List<List<Object>> delSrcTableList = ((List<List<Object>>) jdbcManagerTgt.executeQuery(getDelSrcTableSql, new ListResultHandler(), null, false));
        List<String> delTableNameList = delSrcTableList.stream()
                .map(table -> table.get(0).toString().trim().toUpperCase())
                .collect(Collectors.toList());
        //获取要进行数据删除的表信息
        List<List<Object>> toDelTables = allTableList.stream()
                .filter(table -> delTableNameList.contains(table.get(0).toString().trim().toUpperCase()))
                .collect(Collectors.toList());
        logger.debug("开始从以下表中进行数据记录的删除操作:{}", delTableNameList);
        //进行数据记录的删除
        String delSqlTemplate = "DELETE FROM #targetTable WHERE srcid IN (SELECT srcid FROM TYC_DEL_REC WHERE table_nm = '#srcTable')";
        for (List<Object> tableInfo : toDelTables) {
            String srcTable = tableInfo.get(0).toString().trim().toUpperCase();
            String targetTable = tableInfo.get(1).toString().trim().toUpperCase();
            String delDataSql = delSqlTemplate.replaceAll("#targetTable", targetTable).replaceAll("#srcTable", srcTable);
            logger.debug("删除数据sql: {}", delDataSql);
            int delCount = jdbcManagerTgt.executeUpdate(delDataSql, null, false, false);
            logger.debug("删除了{}表中{}条记录", targetTable, delCount);
        }
        //删除完成后将删除记录表清空
        String deleteRecSql = "DELETE FROM TYC_DEL_REC";
        logger.debug("清空删除记录表数据sql: {}", deleteRecSql);
        int cleanCount = jdbcManagerTgt.executeUpdate(deleteRecSql, null, false, false);
        logger.debug("清空了删除记录表TYC_DEL_REC中{}条记录", cleanCount);
        jdbcManagerTgt.commit();
    }

    /**
     * 发送预警信息
     * 对发送预警逻辑进行单独处理（主要是异常的处理）,减少发送预警逻辑对主逻辑的影响
     *
     * @param subject
     * @param message
     * @param attachmentList
     */
    private void sendAlert(String subject, String message, List<String> attachmentList) {
        try {
            alert.sendAlert(subject, message, attachmentList, null);
        } catch (Exception e) {
            logger.error("报警发送失败！");
            LogUtils.logException(logger, e);
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.error("需要一个参数作为source_code比如cscs");
            System.exit(10);
        }
        String sourceCode = args[0].trim().toLowerCase();
        TargetLoad targetLoad = new TargetLoad(sourceCode);
        int status = targetLoad.start();
        System.exit(status);
    }
}