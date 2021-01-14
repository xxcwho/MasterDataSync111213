package com.chinacscs.etl.stage;

import com.chinacscs.etl.alert.AlertFactory;
import com.chinacscs.etl.alert.IAlert;
import com.chinacscs.jdbc.JdbcManager;
import com.chinacscs.jdbc.ListResultHandler;
import com.chinacscs.params.Constants;
import com.chinacscs.utils.*;
import com.csvreader.CsvReader;
import com.zaxxer.hikari.pool.HikariPool;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

/**
 * @author wangjj
 * @date 2016/12/21 20:41
 * @description 将incoming目录下的所有CSV文件加载进stage表里，每成功加载一个文件就将该文件进行归档，并在日志表中记录一行日志
 * 注意点：
 * 1.当前批次导入完成后，会将incoming文件中的所有.finish移入归档目录
 * 2.可以存在部分文件导出失败的情况，这种情况依然返回状态码0，即会触发TargetLoad，.finish文件也会进行归档，避免DownloadFromSftp程序重新开始下载已经下载完成的文件
 * 3.本批次导入失败的文件会在下一个批次中被重新导入
 * 4.StageLoad时会将client id由0替换成客户具体的client id
 * @copyright(c) chinacscs all rights reserved
 */
public class StageLoad {
    private static final Logger logger = LoggerFactory.getLogger(StageLoad.class);
    JdbcManager jdbcManagerMds;
    JdbcManager jdbcManagerStg;
    JdbcManager jdbcManagerStgMonitor;
    Integer subscribeId;
    String baseDir;
    String archiveDir;
    char delimiter;
    String decoding;
    Boolean withHeader;
    ExecutorService pool;
    Integer clientId;
    String databaseType;
    List<String> errorTableList = new ArrayList<>();
    IAlert alert;
    String logPath = System.getProperty("log.dir") + "/StageLoad.log";
    Integer batchSize;
    /** MDS分发表视图名称 */
    private String subscribeView;
    /** STG库连接信息 */
    private String dburlStg;


    public StageLoad(String sourceCode) {
        String driverMds = PropUtils.getString("mds.jdbc.driver");
        String dburlMds = PropUtils.getString("mds.jdbc.url");
        String userMds = PropUtils.getString("mds.jdbc.user");
        String passwordMds = PropUtils.getString("mds.jdbc.password");
        String driverStg = PropUtils.getString("stage.jdbc.driver");
        dburlStg = PropUtils.getString("stage.jdbc.url");
        String userStg = PropUtils.getString("stage.jdbc.user");
        String passwordStg = PropUtils.getString("stage.jdbc.password");
        databaseType = dburlStg.split(":")[1];
        if (!Constants.SUPPORTED_DATABASE.contains(databaseType.toLowerCase())) {
            logger.error("不支持的数据库:" + databaseType);
            System.exit(-1);
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
            //批量执行时分批执行的size
            batchSize = PropUtils.getInteger("jdbc.batch.size") != null ? PropUtils.getInteger("jdbc.batch.size") : 10000;
            logger.debug("批量执行时分批大小：" + batchSize);
        } catch (ClassNotFoundException e) {
            logger.error("找不到JDBC驱动！");
            LogUtils.logException(logger, e);
            System.exit(1);
        } catch (SQLException e) {
            logger.error("登录失败！请检查mds.jdbc.password参数及login.passowrd参数！");
            LogUtils.logException(logger, e);
            System.exit(1);
        }
        //为兼容mysql的自增主键获取，默认使用单线程池
        Integer defaultPoolSize = 1;
        Integer poolSize = PropUtils.getInteger("stage.threads");
        pool = poolSize == null ? Executors.newFixedThreadPool(defaultPoolSize) : Executors.newFixedThreadPool(poolSize);
        baseDir = PropUtils.getString("stage." + sourceCode + ".baseDir");
        archiveDir = PropUtils.getString("stage." + sourceCode + ".archiveDir");
        delimiter = PropUtils.getString("stage.delimiter").charAt(0);
        decoding = PropUtils.getString("stage.decoding");
        withHeader = PropUtils.getBoolean("stage.withHeader");

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
    }

    private int start() {
        int status = 0;
        boolean[] hasError = {false};
        try {
            //记录本批次开始的时间
            Date batchStartDate = new Date();

            //查出所有需要加载的stage表
            String getStageTableSql = "SELECT STG_TABLE, FILE_NM, STG_FIELD_LIST, PROCESS_TYPE, TGT_PHYSICAL_PK from " + subscribeView + " WHERE ISDEL = 0 AND (CLIENT_ID = ? OR CLIENT_ID IS NULL) AND SUBSCRIBE_ID = ? ";
            List<List<Object>> stageTableList = (List<List<Object>>) jdbcManagerMds.executeQuery(getStageTableSql, new ListResultHandler(), new Object[]{clientId, subscribeId}, true);
            logger.info("即将进行数据加载的stage表数量：{}", stageTableList.size());

            //待加载的数据文件目录
            File baseDirectory = new File(baseDir);
            if (baseDirectory.listFiles(new CsvUtils.CsvFileFilter()).length == 0) {
                logger.info("没有需要加载的Stage表文件，程序退出...");
                //当目录中没有需要加载的csv文件时，直接退出加载过程，不执行后续操作
                return 0;
            }

            //查出当前的BATCHSID
            String getBatchSidSql = null;
            if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                getBatchSidSql = "SELECT seq_etl_stg_loadbatch.nextval from dual";
            } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                getBatchSidSql = "SELECT NEXTVAL('seq_etl_stg_loadbatch')";
            } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                getBatchSidSql = "select ifnull(max(loadbatch_sid),0)+1 from etl_stg_run_details";
            }
            Long BatchSid = -1L;
            BatchSid = (Long) jdbcManagerStgMonitor.executeQuery(getBatchSidSql, rs -> {
                rs.next();
                return rs.getLong(1);
            }, null, false);
            final Long FinalBatchSid=BatchSid;
            String MonitorlogInsertSql="insert into ETL_STG_RUN_DETAILS(run_details_sid,process_nm,loadbatch_sid,received_file,dest_table,begin_dt,run_status,is_del,insert_dt,updt_dt) values(?,'MasterDataSync',?,?,?,?,0,0,?,?)";
            String MointorlogUpdateSql="update ETL_STG_RUN_DETAILS set received_dt=?,orig_record_count=?,record_count=?,end_dt=?,run_status=1,updt_dt=? where run_details_sid=?";
            String MointorlogUpdateErrorSql="update ETL_STG_RUN_DETAILS set err_message=?,updt_dt=?,run_status=-1 where run_details_sid=?";


            for (List<Object> table : stageTableList) {
                //每一个stage表启动一个线程进行导入
                pool.execute(() -> {
                    String stageTable = table.get(0).toString().trim().toLowerCase();
                    String fileName = table.get(1).toString().trim().toLowerCase();
                    String stageField = table.get(2).toString().trim().toLowerCase();
                    String processType = table.get(3).toString().trim().toLowerCase();
                    String targetPhysicalPk = table.get(4).toString().trim().toLowerCase();
                    List<String> stageFieldList = stream(stageField.split(",")).map(filed -> filed.trim()).collect(Collectors.toList());

                    try {
                        //找出该stage表应该导入的文件
                        List<File> files = stream(baseDirectory.listFiles())
                                .filter(file -> file.isFile() && file.getName().toLowerCase().endsWith("csv"))
                                .filter(file -> stream(file.getName().toLowerCase().split("_"))
                                        .filter(s -> !s.matches("[0-9]+\\.csv") && !"stg".equals(s))
                                        .collect(Collectors.joining("_"))
                                        .equals(fileName))
                                .collect(Collectors.toList());

                        //拼接出插入stage表的insert语句
                        String insertStageSql = "INSERT INTO " + stageTable.toUpperCase() + "(" + stageField + ")" + " VALUES(";
                        if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                            insertStageSql = insertStageSql + stageTable.replace("stg_", "seq_") + ".NEXTVAL, ";
                        } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                            insertStageSql = insertStageSql + "NEXTVAL('" + "seq_" + stageTable + "'), ";
                        } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                            //Mysql数据库对RECORD_SID采用自增处理,所以在插入时不插入RECORD_SID字段
                            String mysqlFieldStr = stream(stageField.split(",")).skip(1).collect(Collectors.joining(","));
                            insertStageSql = "INSERT INTO " + stageTable.toUpperCase() + "(" + mysqlFieldStr + ")" + " VALUES(";
                        }

                        int columnCount = stageFieldList.size();
                        //-1是因为不包含record_sid
                        for (int i = 0; i < columnCount - 1; i++) {
                            insertStageSql = insertStageSql + "?,";
                        }
                        insertStageSql = insertStageSql.replaceAll(",$", "") + ")";
                        logger.debug(insertStageSql);

                        //如果是oracle数据库，需要对日期类这里查出日期类型的索引号
                        Map<Integer, String> dateTypeIndexMap = new HashMap<>(16);
                        if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                            String getdateTypeIndexSql = "SELECT COLUMN_ID, COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = UPPER(?) AND (DATA_TYPE='DATE' OR DATA_TYPE LIKE 'TIMESTAMP%')";
                            jdbcManagerStg.executeQuery(getdateTypeIndexSql, rs -> {
                                while (rs.next()) {
                                    int columnId = rs.getInt(1);
                                    String columnName = rs.getString(2);
                                    dateTypeIndexMap.put(columnId, columnName);
                                }
                                return null;
                            }, new Object[]{stageTable}, false);
                        }

                        //对每个文件进行导入
                        logger.info("即将导入" + stageTable + "表的文件个数：" + files.size());
                        for (File file : files) {
                            Long logSid = -1L;
                            final int[] origRecordCount = {0};
                            int recordCount = -1;
                            Timestamp startDate = new Timestamp(System.currentTimeMillis());
                            Timestamp endDate = null;

                            //查出当前的MONITORLOGSID
                            String getMonitorLogSidSql = null;
                            if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                                getMonitorLogSidSql = "SELECT seq_etl_stg_run_details.nextval from dual";
                            } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                                getMonitorLogSidSql = "SELECT NEXTVAL('seq_etl_stg_run_details')";
                            } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                                //获取STG连接库的Schema名称
                                String stgSchema = dburlStg.substring(dburlStg.lastIndexOf("/") + 1);
                                getMonitorLogSidSql = "SELECT auto_increment FROM information_schema.`TABLES` WHERE TABLE_SCHEMA='" + stgSchema + "' AND TABLE_NAME='ETL_STG_RUN_DETAILS'";
                            }
                            Long MonitorlogSid = -1L;
                            MonitorlogSid = (Long) jdbcManagerStgMonitor.executeQuery(getMonitorLogSidSql, rs -> {
                                rs.next();
                                return rs.getLong(1);
                            }, null, false);

                            try {
                                //查出当前的LOGSID
                                String getLogSidSql = null;
                                if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                                    getLogSidSql = "SELECT seq_etl_stg_loadlog.nextval from dual";
                                } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                                    getLogSidSql = "SELECT NEXTVAL('seq_etl_stg_loadlog')";
                                } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
                                    //获取STG连接库的Schema名称
                                    String stgSchema = dburlStg.substring(dburlStg.lastIndexOf("/") + 1);
                                    getLogSidSql = "SELECT auto_increment FROM information_schema.`TABLES` WHERE TABLE_SCHEMA='" + stgSchema + "' AND TABLE_NAME='ETL_STG_LOADLOG'";
                                }
                                logSid = (Long) jdbcManagerStg.executeQuery(getLogSidSql, rs -> {
                                    rs.next();
                                    return rs.getLong(1);
                                }, null, false);



                                Timestamp MonitorBeginDate = new Timestamp(System.currentTimeMillis());
                                Object[] MonitorlogInsertParams = new Object[]{
                                        MonitorlogSid,
                                        FinalBatchSid,
                                        file.getName(),
                                        stageTable,
                                        MonitorBeginDate,
                                        MonitorBeginDate,
                                        MonitorBeginDate
                                };
                                jdbcManagerStgMonitor.executeUpdate(MonitorlogInsertSql, MonitorlogInsertParams, false, false);
                                jdbcManagerStgMonitor.commit();

                                //读取CSV
                                logger.info("即将导入" + file.toString() + "进" + stageTable);
                                CsvReader csvReader = CsvUtils.getCSVReader(file.toString(), delimiter, decoding, withHeader);
                                List<Object[]> rowList = new ArrayList<>();
                                Long finalLogSid = logSid;
                                Map<Integer, String> finaldateTypeIndexMap = dateTypeIndexMap;
                                CsvUtils.traverseCSV(csvReader, reader1 -> {
                                    List<Object> row = new ArrayList<>();
                                    //-2是因为csv文件中没有logsid也没有record_sid
                                    for (int i = 0; i < columnCount - 2; i++) {
                                        String data = reader1.get(i);
                                        //空字符串替换成NULL
                                        data = (data == null || "".equals(data)) ? null : data;

                                        //oracle数据库需要对日期类型特殊处理
                                        if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                                            if (data != null && finaldateTypeIndexMap.containsKey(i + 2)) {
                                                data = data.replaceAll("\\.[0-9]+$", "");
                                                try {
                                                    if (data.contains(":")) {
                                                        //时间类型
                                                        row.add(new Timestamp(DateUtils.toDate(data, "yyyy-MM-dd HH:mm:ss").getTime()));
                                                    } else { //日期类型
                                                        row.add(new Timestamp(DateUtils.toDate(data, "yyyy-MM-dd").getTime()));
                                                    }
                                                    continue;
                                                } catch (ParseException e) {
                                                    logger.error(stageTable + "表中的" + finaldateTypeIndexMap.get(i + 2) + "列的日期类型数据" + data + "解析失败");
                                                }
                                            }
                                        }
                                        row.add(data);
                                    }
                                    row.add(finalLogSid);
                                    rowList.add(row.toArray());
                                    origRecordCount[0]++;
                                });
                                csvReader.close();
                                logger.info("文件： {} 解析完毕，开始进行数据导入...", file.getName());

                                recordCount = jdbcManagerStg.executeBatchWithBatchSize(insertStageSql, rowList, false, false, batchSize);
                                logger.info("已成功将" + file.toString() + "导入" + stageTable + "表，导入行数：" + recordCount);
                                //更新client_id
                                if (stageFieldList.contains("client_id")) {
                                    jdbcManagerStg.executeUpdate("update " + stageTable.toUpperCase() + " set client_id = ?", new Object[]{clientId}, false, false);
                                    logger.info("成功更新client_id为" + clientId);
                                }
                                endDate = new Timestamp(System.currentTimeMillis());
                                //导入成功后将文件归档
                                FileUtils.deleteQuietly(new File(archiveDir + "/" + file.getName()));
                                FileUtils.moveFileToDirectory(file, new File(archiveDir), true);
                                logger.info("已将" + file.toString() + "移入" + archiveDir);
                                logger.info(stageTable + "表已加载完毕！");

                                String receivedDateStr = file.getName().substring(file.getName().lastIndexOf('_') + 1).replaceAll("\\.csv$", "");
                                Timestamp receivedDate = new Timestamp(DateUtils.toDate(receivedDateStr, "yyyyMMddHHmmss").getTime());
                                Timestamp MonitorEndDate = new Timestamp(System.currentTimeMillis());
                                Object[] MonitorlogUpdateParams = new Object[]{
                                        receivedDate,
                                        origRecordCount[0],
                                        recordCount,
                                        MonitorEndDate,
                                        MonitorEndDate,
                                        MonitorlogSid

                                };
                                jdbcManagerStgMonitor.executeUpdate(MointorlogUpdateSql, MonitorlogUpdateParams, false, false);
                                jdbcManagerStgMonitor.commit();

                            } catch (Exception e) {
                                hasError[0] = true;
                                recordCount = -1;
                                errorTableList.add(stageTable);
                                logger.error(stageTable + "表插入失败！！");
                                LogUtils.logException(logger, e);
                                try {
                                    jdbcManagerStg.rollback();
                                    Timestamp MonitorErrorEndDate = new Timestamp(System.currentTimeMillis());
                                    Object[] MonitorErrorlogUpdateParams = new Object[]{
                                            e.toString(),
                                            MonitorErrorEndDate,
                                            MonitorlogSid
                                    };
                                    jdbcManagerStgMonitor.executeUpdate(MointorlogUpdateErrorSql, MonitorErrorlogUpdateParams, false, false);
                                    jdbcManagerStgMonitor.commit();
                                } catch (Exception e1) {
                                    LogUtils.logException(logger, e1);
                                }

                            }
                            //记录log
                            String logSql = "INSERT INTO ETL_STG_LOADLOG(loadlog_sid,process_nm,received_file,received_dt,orig_record_count,record_count,start_dt,end_dt) VALUES(?, 'MasterDataSync', ?, ?, ?, ?, ?, ?)";
                            logger.debug(logSql);
                            String receivedDateStr = file.getName().substring(file.getName().lastIndexOf('_') + 1).replaceAll("\\.csv$", "");
                            Timestamp receivedDate = new Timestamp(DateUtils.toDate(receivedDateStr, "yyyyMMddHHmmss").getTime());
                            Object[] params = new Object[]{
                                    logSid,
                                    file.getName(),
                                    receivedDate,
                                    origRecordCount[0],
                                    recordCount,
                                    startDate,
                                    endDate
                            };
                            jdbcManagerStg.executeUpdate(logSql, params, false, false);
                            jdbcManagerStg.commit();
                        }
                    } catch (Exception e) {
                        LogUtils.logException(logger, e);
                        hasError[0] = true;
                        errorTableList.add(stageTable);
                    } finally {
                        try {
                            jdbcManagerMds.close();
                            jdbcManagerStg.close();
                            jdbcManagerStgMonitor.close();
                        } catch (Exception e) {
                            LogUtils.logException(logger, e);
                        }
                    }
                });
            }
            pool.shutdown();
            while (!pool.awaitTermination(5, TimeUnit.SECONDS)) {
            }
            //将.finish文件移动进归档目录
            IOFileFilter finishFileFilter = new IOFileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.getName().endsWith(".finish");
                }

                @Override
                public boolean accept(File dir, String name) {
                    return false;
                }
            };
            for (File finishFile : FileUtils.listFiles(baseDirectory, finishFileFilter, finishFileFilter)) {
                FileUtils.deleteQuietly(new File(archiveDir + "/" + finishFile.getName()));
                FileUtils.moveFileToDirectory(finishFile, new File(archiveDir), true);
            }
            logger.info("全部stage表加载完毕");

            //勾稽校对
            if (!"off".equals(PropUtils.getString("stage.financeCheck"))) {
                compyFinanceCheck(batchStartDate, baseDirectory);
            }
            String compyFinanceCheckReportPath = baseDirectory + "/CompanyFinanceCheckReport_" + DateUtils.toString(batchStartDate, "yyyyMMddHHmmss") + ".xlsx";

            //生成报告文件
            logger.info("开始生成本次stage表加载情况的统计报告...");
            String generateReportSql = "SELECT loadlog_sid,process_nm,received_file,received_dt,orig_record_count,record_count,start_dt,end_dt FROM ETL_STG_LOADLOG where start_dt >= ?";
            logger.debug(generateReportSql);
            List<List<Object>> resultList = (List<List<Object>>) jdbcManagerStg.executeQuery(generateReportSql, new ListResultHandler(), new Object[]{new Timestamp(batchStartDate.getTime())}, true);
            String reportPath = baseDirectory + "/StageLoadReport_" + DateUtils.toString(batchStartDate, "yyyyMMddHHmmss") + ".csv";
            List<String[]> contents = resultList.stream()
                    .map(innerList -> innerList.stream()
                            .map(obj -> obj == null ? null : obj.toString())
                            .toArray(obj -> new String[innerList.size()]))
                    .collect(Collectors.toList());
            String header = "loadlog_sid,process_nm,received_file,received_dt,orig_record_count,record_count,start_dt,end_dt";
            contents.add(0, header.split(","));
            CsvUtils.writeCsv(reportPath, ',', "UTF8", contents);

            //发送报警
            try {
                String logPath = System.getProperty("log.dir") + "/StageLoad.log";
                String subject;
                String message = "开始时间：" + DateUtils.toString(batchStartDate, "yyyy-MM-dd HH:mm:ss") + "\n结束时间：" + DateUtils.getCurrentTimePretty();
                if (hasError[0]) {
                    subject = "[S" + subscribeId + "C" + clientId + "]向staging数据库中加载数据的过程中发生了错误！";
                    logger.error(subject);
                    message = message + "\n以下表加载失败，请查看日志获取详细信息";
                    for (String errorTable : errorTableList) {
                        message += "\n" + errorTable;
                    }
                    sendAlert(subject, message, Arrays.asList(logPath, reportPath));
                } else {
                    subject = "[S" + subscribeId + "C" + clientId + "]数据已加载至staging数据库";
                    logger.info(subject);
                    if (!"off".equals(PropUtils.getString("stage.financeCheck"))) {
                        sendAlert(subject, message, Arrays.asList(logPath, compyFinanceCheckReportPath, reportPath));
                    } else {
                        sendAlert(subject, message, Arrays.asList(logPath, reportPath));
                    }
                }
            } catch (Exception e) {
                logger.error("报警发送失败！");
                LogUtils.logException(logger, e);
            }
        } catch (Exception e) {
            status = 1;
            LogUtils.logException(logger, e);
            //发送报警
            String subject = "[S" + subscribeId + "C" + clientId + "]向staging数据库中加载数据的过程中发生了错误！";
            String message = "请查看附件中的日志获取详细信息。。";
            sendAlert(subject, message, Arrays.asList(logPath));
        } finally {
            try {
                pool.shutdown();
                jdbcManagerStg.close();
            } catch (SQLException e) {
                LogUtils.logException(logger, e);
            }
        }
        return status;
    }

    private void compyFinanceCheck(Date batchStartDate, File directory) throws SQLException, IOException, ParseException {
        //开始执行勾稽校对
        logger.info("开始执行勾稽校对...");
        String compyFinanceCheckSql = "";
        if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
            compyFinanceCheckSql = "select fn_compy_finance_check() from dual";
        } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
            compyFinanceCheckSql = "select fn_compy_finance_check()";
        } else if (Constants.DB_TYPE_MYSQL.equals(databaseType)) {
            compyFinanceCheckSql = "select fn_compy_finance_check()";
        }
        jdbcManagerStg.executeQuery(compyFinanceCheckSql, rs -> null, null, false);
        jdbcManagerStg.commit();

        //查询报告
        logger.info("开始查询勾稽校对报告...");
        Map<String, List<List<Object>>> rowListMap = new HashMap<>(16);
        String getReportSql = "select * from (select * from EXP_COMPY_FINANCE_CHECK_REPORT limit 1) a union all select * from EXP_COMPY_FINANCE_CHECK_REPORT where check_tbl_nm = ";
        List<List<Object>> companyBalancesheetRowList = (List<List<Object>>) jdbcManagerStg.executeQuery(getReportSql + "'资产负债表'", new ListResultHandler(), null, false);
        logger.info("资产负债表不通过条数：" + Integer.valueOf(companyBalancesheetRowList.size() - 1));
        rowListMap.put("资产负债表", companyBalancesheetRowList);
        List<List<Object>> companyIncomestateList = (List<List<Object>>) jdbcManagerStg.executeQuery(getReportSql + "'利润表'", new ListResultHandler(), null, false);
        logger.info("利润表不通过条数：" + Integer.valueOf(companyIncomestateList.size() - 1));
        rowListMap.put("利润表", companyIncomestateList);
        List<List<Object>> companyCashflowRowList = (List<List<Object>>) jdbcManagerStg.executeQuery(getReportSql + "'现金流量表'", new ListResultHandler(), null, false);
        logger.info("利润表不通过条数：" + Integer.valueOf(companyCashflowRowList.size() - 1));
        rowListMap.put("现金流量表", companyCashflowRowList);
        String getRuleSql = "select '检查表名称','适用财务模板类型','规则代码','规则中文公式' union all select * from VW_EXP_STG_LKP_FINANCE_CHECK_RULE ";
        List<List<Object>> ruleList = (List<List<Object>>) jdbcManagerStg.executeQuery(getRuleSql, new ListResultHandler(), null, true);
        rowListMap.put("勾稽校对规则-代码映射", ruleList);

        //生成报告对应的EXCEL文件
        logger.info("开始生成勾稽校对报告...");
        Workbook workbook = new XSSFWorkbook();
        for (Map.Entry<String, List<List<Object>>> rowListEntry : rowListMap.entrySet()) {
            String sheetName = rowListEntry.getKey();
            List<List<Object>> rowList = rowListEntry.getValue();

            Sheet sheet = workbook.createSheet(sheetName);
            for (int i = 0; i < rowList.size(); i++) {
                Row row = sheet.createRow(i);
                List<Object> dataList = rowList.get(i);
                for (int j = 0; j < dataList.size(); j++) {
                    Cell cell = row.createCell(j, Cell.CELL_TYPE_STRING);
                    cell.setCellValue(dataList.get(j).toString());
                }
            }
        }
        String compyFinanceCheckReportPath = directory + "/CompanyFinanceCheckReport_" + DateUtils.toString(batchStartDate, "yyyyMMddHHmmss") + ".xlsx";
        FileOutputStream fos = new FileOutputStream(compyFinanceCheckReportPath);
        workbook.write(fos);

        jdbcManagerStg.close();
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
        StageLoad stageLoad = new StageLoad(sourceCode);
        int status = stageLoad.start();
        System.exit(status);
    }
}