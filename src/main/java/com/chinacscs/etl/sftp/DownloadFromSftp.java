package com.chinacscs.etl.sftp;

import com.chinacscs.etl.alert.AlertFactory;
import com.chinacscs.etl.alert.IAlert;
import com.chinacscs.jdbc.JdbcManager;
import com.chinacscs.jdbc.ListResultHandler;
import com.chinacscs.params.Constants;
import com.chinacscs.utils.*;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.zaxxer.hikari.pool.HikariPool;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author wangjj
 * @date 2017/12/18 18:26
 * @description 先从本地目录下找到最新下载的.finish文件的时间戳，在和sftp目录中最新导出的.finish文件的时间戳对比，如果较小，就认为有新的文件，开始执行
 * 本下载程序；否则就认为没有新的文件，直接退出本程序，下载完成后会
 * 注意：
 * 1.下载是串行下载
 * 2.所有文件全部下载成功后会将下载的目录中的.finish文件，但.finish文件的时间戳不能能大于当前批次开始时最大的.finish文件的时间戳（大于的那些是开始当前批次后新生成的.finish文件，放到下一个批次再下载）
 * 3.如果当前批次有文件下载失败或StageLoad程序还未对本批次下载完成的文件进行归档，则当前批次已经下载成功的文件下个批次还会再下载
 * @copyright(c) chinacscs all rights reserved
 */
public class DownloadFromSftp {
    private static final Logger logger = LoggerFactory.getLogger(DownloadFromSftp.class);
    ChannelSftp sftp;
    Integer subscribeId;
    Integer clientId;
    String remoteSubscribeSftpDir;
    String remoteClientSftpDir;
    String localDir;
    String archiveDir;
    Map<String, String> downloadedFilesMap = new ConcurrentHashMap<>();
    JdbcManager jdbcManagerMds;
    String databaseType;
    IAlert alert;
    String logPath = System.getProperty("log.dir") + "/DownloadFromSftp.log";
    /** MDS分发表视图名称 */
    private String subscribeView;

    /** 未找到最新的finish文件时默认最大时间 */
    static final long DEFAULT_MAX_TIME = 19000000000000L;
    /** 未找到最新的文件目录时默认最大日期 */
    static final int DEFAULT_MAX_DATE = 19000000;

    public DownloadFromSftp() {
        try {
            try {
                alert = AlertFactory.getAlert();
            } catch (Exception e) {
                logger.error("初始化Alert实例失败！");
                System.exit(-1);
            }
            logger.info("开始初始化连接.....");
            String dburlMds = PropUtils.getString("mds.jdbc.url");
            databaseType = dburlMds.split(":")[1];
            if (!Constants.SUPPORTED_DATABASE.contains(databaseType.toLowerCase())) {
                logger.error("不支持的数据库:" + databaseType);
                System.exit(-1);
            }
            String driverMds = PropUtils.getString("mds.jdbc.driver");
            String userMds = PropUtils.getString("mds.jdbc.user");
            String passwordMds = PropUtils.getString("mds.jdbc.password");
            try {
                jdbcManagerMds = new JdbcManager(dburlMds, userMds, passwordMds, driverMds);
            } catch (HikariPool.PoolInitializationException e) {
                logger.error("MDS数据库连接建立失败！");
                LogUtils.logException(logger, e);
                //发送预警
                String subject = "[S" + subscribeId + "C" + clientId + "]MDS数据库连接失败！";
                String message = "请查看附件中的日志获取详细信息...";
                sendAlert(subject, message, Arrays.asList(logPath));
                System.exit(-1);
            }

            logger.info("尝试进行登录验证....");
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
                throw new SQLException("login.passowrd不正确！");
            }
            clientId = Integer.valueOf(info.get(0).get(0).toString());
            subscribeId = Integer.valueOf(info.get(0).get(1).toString());
            logger.info("客户端ID是" + clientId);
            logger.info("分发标识符是" + subscribeId);
            logger.info("登录验证成功....");

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

            //连接sftp
            Integer sftpPort = PropUtils.getInteger("sftp.port");
            String sftpHost = PropUtils.getString("sftp.host");
            String privateKey = PropUtils.getString("sftp.prvkey");
            sftp = SftpUtils.getConnectionByKey("c" + clientId.toString(), privateKey, sftpHost, sftpPort);
            logger.info("sftp服务器连接成功！");

            //获取sftp下载路径、客户端本地保存路径
            remoteSubscribeSftpDir = PropUtils.getString("sftp.remoteDirectory") + "/s" + subscribeId;
            remoteClientSftpDir = PropUtils.getString("sftp.remoteDirectory") + "/c" + clientId;
            localDir = PropUtils.getString("sftp.localDirectory");
            //根据同步过程执行配置决定备份目录(主用是于获取本地finish文件)
            Integer syncStep = PropUtils.getOrDefault("sync_step", Constants.SYNC_STEP_LOADTODB);
            if (syncStep.equals(Constants.SYNC_STEP_DOWNLOADFILE)) {
                archiveDir = localDir;
            } else {
                archiveDir = PropUtils.getString("stage.cscs.archiveDir");
            }
        } catch (JSchException e) {
            logger.error("sftp连接失败！");
            LogUtils.logException(logger, e);
            //发送预警
            String subject = "[S" + subscribeId + "C" + clientId + "]sftp连接失败！";
            String message = "请查看附件中的日志获取详细信息。。";
            sendAlert(subject, message, Arrays.asList(logPath));
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            logger.error("找不到JDBC驱动！");
            LogUtils.logException(logger, e);
            System.exit(1);
        } catch (SQLException e) {
            logger.error("登录失败！请检查mds.jdbc.password参数及login.passowrd参数！");
            LogUtils.logException(logger, e);
            System.exit(1);
        }
    }

    @SuppressWarnings("unchecked")
    int start() {
        int status = 0;
        final boolean[] errorFlag = {false};
        try {
            //找出已下载的最新.finish文件时间戳: 先截出finish文件名中的时间,再按时间倒序排序
            Long maxDownloadedFinishTimestamp = Arrays.stream(new File(archiveDir).listFiles())
                    .map(File::getName)
                    .filter(fileName -> fileName.endsWith(".finish"))
                    .map(fileName -> fileName.replaceAll("\\.finish$", ""))
                    .map(Long::parseLong)
                    .sorted(Comparator.comparing(Long::longValue).reversed())
                    .findFirst()
                    .orElse(DEFAULT_MAX_TIME);
            //找出sftp中对应client的最新的文件夹: 先截出sftp client目录文件名中的时间,再按时间倒序排序
            Integer maxRemoteSubDir = ((List<String>) (sftp.ls(remoteClientSftpDir)
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.toList())))
                    .stream()
                    .filter(info -> info.startsWith("d") && !info.endsWith("."))
                    .map(info -> info.split(" +")[8])
                    .map(Integer::parseInt)
                    .sorted(Comparator.comparing(Integer::intValue).reversed())
                    .findFirst()
                    .orElse(DEFAULT_MAX_DATE);
            //然后从最新的sftp夹找到最新的文件的时间戳
            Long maxSftpFinishTimestamp;
            if (maxRemoteSubDir == DEFAULT_MAX_DATE) {
                maxSftpFinishTimestamp = DEFAULT_MAX_TIME;
            } else {
                //找出sftp中对应client目录下最新.finish文件时间戳: 先截出finish文件名中的时间,再按时间倒序排序
                maxSftpFinishTimestamp = ((List<String>) (sftp.ls(remoteClientSftpDir + "/" + maxRemoteSubDir)
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.toList())))
                        .stream()
                        .filter(info -> info.endsWith(".finish"))
                        .map(info -> info.split(" +")[8])
                        .map(fileName -> fileName.replaceAll("\\.finish$", ""))
                        .map(Long::parseLong)
                        .sorted(Comparator.comparing(Long::longValue).reversed())
                        .findFirst()
                        .orElse(DEFAULT_MAX_TIME);
            }
            logger.debug("sftp最新.finish时间戳：" + maxSftpFinishTimestamp);
            logger.debug("本地最新.finish时间戳：" + maxDownloadedFinishTimestamp);
            //如果sftp上没有比已下载文件更新的文件，直接返回
            if (maxDownloadedFinishTimestamp >= maxSftpFinishTimestamp) {
                logger.warn("远程sftp不存在新的需要下载的文件，程序退出........");
                status = -1;
                return status;
            }

            //开始下载
            String downloadStartTime = DateUtils.getCurrentTime();
            Set<String> traversedDirectorySet = new HashSet<>();
            //查找所有即将导出的stage表的列表
            String getStageTableInfoListSql = "SELECT STG_TABLE,SUBSCRIBE_LEVEL FROM " + subscribeView + " WHERE ISDEL = 0 AND (CLIENT_ID = ? OR CLIENT_ID IS NULL) AND SUBSCRIBE_ID = ? ";
            List<List<Object>> stageTableInfoList = (List<List<Object>>) jdbcManagerMds.executeQuery(getStageTableInfoListSql, new ListResultHandler(), new Object[]{clientId, subscribeId}, true);
            logger.info("即将进行数据文件下载的stage表数量：{}", stageTableInfoList.size());

            //并行下载所有stage表对应的csv文件
            stageTableInfoList.forEach(stageTableInfo -> {
                String stageTable = stageTableInfo.get(0).toString().trim().toLowerCase();
                Integer subscribeLevel = Integer.parseInt(stageTableInfo.get(1).toString());
                logger.info("开始下载" + stageTable + "对应的csv文件....");
                //根据分发级别去不同的目录查找文件
                String remoteSftpDir;
                if (subscribeLevel == 0) {
                    //表级
                    remoteSftpDir = remoteSubscribeSftpDir;
                } else { //行级
                    remoteSftpDir = remoteClientSftpDir;
                }
                try {
                    //本地最新.finish文件时间戳对应的日期（yyyyMMdd）,用以过滤sftp服务器中的文件夹
                    Integer maxDownloadedFinishDate = Integer.valueOf(maxDownloadedFinishTimestamp.toString().substring(0, 8));
                    //找出下载地址中和maxDownloadedFinishTimestamp相同时间或更新的文件夹: 先截出sftp client目录文件名中的时间,再与本地最新的finish文件时间比较
                    List<Integer> downloadDirs = ((List<String>) (sftp.ls(remoteSftpDir)
                            .stream()
                            .map(Object::toString)
                            .collect(Collectors.toList())))
                            .stream()
                            .filter(info -> info.startsWith("d") && !info.endsWith("."))
                            .map(info -> info.split(" +")[8])
                            .map(Integer::parseInt)
                            .filter(date -> date >= maxDownloadedFinishDate)
                            .collect(Collectors.toList());
                    //遍历每个文件夹进行下载
                    for (Integer dir : downloadDirs) {
                        String downloadDir = remoteSftpDir + "/" + dir;
                        //获取目录中将要下载的文件： 取文件名与Stage表名称匹配的文件,时间戳小于maxDownloadedFinishTimestamp的文件不会被下载,并且当前批次开始后新生成的.finish文件不会下载
                        List<String> fileToDownLoad = ((List<String>) (sftp.ls(downloadDir)
                                .stream()
                                .map(Object::toString)
                                .collect(Collectors.toList())))
                                .stream()
                                .map(info -> info.split(" +")[8])
                                .filter(fileName -> fileName.matches(stageTable + "_" + "[0-9]+\\.csv") && fileName.endsWith(".csv"))
                                .filter(fileName -> {
                                    //时间戳小于maxDownloadedFinishTimestamp的文件不会被下载，并且当前批次开始后新生成的文件不会下载
                                    Long dataFileExportTime = Long.parseLong(fileName.replace(stageTable + "_", "").replace(".csv", ""));
                                    return dataFileExportTime > maxDownloadedFinishTimestamp && dataFileExportTime <= maxSftpFinishTimestamp;
                                }).collect(Collectors.toList());
                        //获取目录中将要下载的文件数量
                        long fileCount = fileToDownLoad.size();
                        logger.info("该目录:{} 下需要下载的：{} 对应的csv文件数量为：{}", downloadDir, stageTable, fileCount);
                        //将该目录记为已经下载
                        traversedDirectorySet.add(downloadDir);
                        //不加这一行会导致如果本批次没有按客户分法的数据，.finish文件将不会下载
                        traversedDirectorySet.add(remoteClientSftpDir + "/" + dir);
                        //如果目录下没有要下载的文件就直接跳过
                        if (fileCount == 0) {
                            continue;
                        }

                        logger.info("开始将sftp服务器上" + downloadDir + "目录下的所有" + stageTable + "表对应的文件下载至" + localDir + "....");

                        //遍历目录下的所有文件，开始下载
                        fileToDownLoad.forEach(fileName -> {
                            String srcPath = downloadDir + "/" + fileName;
                            String distPath = localDir + "/" + fileName;
                            try {
                                sftp.get(srcPath, distPath);
                                logger.info("已成功将" + srcPath + "下载至" + distPath);
                            } catch (SftpException e) {
                                errorFlag[0] = true;
                                logger.error(srcPath + "下载失败！");
                                LogUtils.logException(logger, e);
                            }
                            //记录下载文件的行数及下载完成的时间
                            Integer rowCount = null;
                            try {
                                rowCount = CsvUtils.getRowCount(distPath, PropUtils.getString("stage.delimiter").charAt(0), PropUtils.getString("stage.decoding"), PropUtils.getBoolean("stage.withHeader"));
                                String downloadTime = DateUtils.getCurrentTimePretty();
                                downloadedFilesMap.put(distPath, rowCount + "," + downloadTime);
                            } catch (IOException e) {
                                errorFlag[0] = true;
                                rowCount = -1;
                                logger.error(distPath + "行数统计失败！");
                                LogUtils.logException(logger, e);
                            }
                        });
                    }
                    logger.info(stageTable + "对应的csv文件已经下载完毕");
                } catch (SftpException e) {
                    errorFlag[0] = true;
                    LogUtils.logException(logger, e);
                }
            });
            //将本次下载的文件的文件名和行数记录到countFile里
            File countFile = new File(localDir + "/count_" + downloadStartTime + ".csv");
            List<String> countDataList = new ArrayList<>();
            countDataList.add("file_name,row_count,download_time" + System.lineSeparator());
            for (Map.Entry<String, String> entry : downloadedFilesMap.entrySet()) {
                String fileName = entry.getKey();
                countDataList.add(fileName + "," + entry.getValue() + System.lineSeparator());
            }
            FileUtils.writeLines(countFile, PropUtils.getString("stage.decoding"), countDataList, true);
            logger.info(countFile.getCanonicalPath() + "生成完毕");

            if (errorFlag[0]) {
                logger.error("部分文件拉取失败...");
                status = 1;
            } else {
                //将遍历的目录中的.finish文件下载到本地：先截出finish文件名中的时间,当前批次开始后新生成的.finish文件不会下载
                logger.info("开始下载.finish文件...");
                for (String traversedDirectory : traversedDirectorySet) {
                    ((List<String>) (sftp.ls(traversedDirectory + "/*.finish")
                            .stream()
                            .map(Object::toString)
                            .collect(Collectors.toList())))
                            .stream()
                            .filter(info -> info.endsWith(".finish"))
                            .map(info -> info.split(" +")[8])
                            .map(fileName -> fileName.replaceAll("\\.finish$", ""))
                            .map(Long::parseLong)
                            .filter(finishTimestamp -> finishTimestamp <= maxSftpFinishTimestamp && finishTimestamp > maxDownloadedFinishTimestamp)
                            .map(finishTimestamp -> finishTimestamp + ".finish")
                            .forEach(fileName -> {
                                try {
                                    sftp.get(traversedDirectory + "/" + fileName, localDir);
                                    logger.info("成功将" + fileName + "下载至" + localDir + "/" + fileName);
                                } catch (SftpException e) {
                                    LogUtils.logException(logger, e);
                                }
                            });
                }
            }
            logger.info("全部文件下载完毕！");

            //发送报警
            try {
                String logPath = System.getProperty("log.dir") + "/DownloadFromSftp.log";
                String countFilePath = countFile.getCanonicalPath();
                String subject;
                String message = "开始时间：" + DateUtils.toString(DateUtils.toDate(downloadStartTime, "yyyyMMddHHmmss"), "yyyy-MM-dd HH:mm:ss") + "\n结束时间：" + DateUtils.getCurrentTimePretty();
                if (errorFlag[0]) {
                    subject = "[S" + subscribeId + "C" + clientId + "]数据从中证服务器拉取至本地时遇到了错误！";
                    logger.error(subject);
                    message = message + "\n请查看日志获取详细详细";
                    sendAlert(subject, message, Arrays.asList(logPath));
                } else {
                    subject = "[S" + subscribeId + "C" + clientId + "]数据已从中证SFTP服务器上拉取至本地！";
                    logger.info(subject);
                    sendAlert(subject, message, Arrays.asList(logPath, countFilePath));
                }
            } catch (Exception e) {
                logger.error("报警发送失败！");
                LogUtils.logException(logger, e);
            }
        } catch (IOException | SftpException | SQLException e) {
            status = 1;
            LogUtils.logException(logger, e);
            //发送预警
            String subject = "[S" + subscribeId + "C" + clientId + "]数据从中证服务器拉取至本地时遇到了错误！";
            String message = "请查看附件中的日志获取详细信息。。";
            sendAlert(subject, message, Arrays.asList(logPath));
        } finally {
            try {
                SftpUtils.close(sftp);
                logger.info("已断开sftp服务器");
                jdbcManagerMds.close();
            } catch (JSchException | SQLException e) {
                LogUtils.logException(logger, e);
            }
        }
        return status;
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
        DownloadFromSftp downloadFromSftp = new DownloadFromSftp();
        int status = downloadFromSftp.start();
        System.exit(status);
    }
}