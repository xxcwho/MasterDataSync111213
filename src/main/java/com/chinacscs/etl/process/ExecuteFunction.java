package com.chinacscs.etl.process;

import com.chinacscs.etl.alert.AlertFactory;
import com.chinacscs.etl.alert.IAlert;
import com.chinacscs.jdbc.JdbcManager;
import com.chinacscs.jdbc.ListResultHandler;
import com.chinacscs.params.Constants;
import com.chinacscs.utils.LogUtils;
import com.chinacscs.utils.PropUtils;
import com.chinacscs.utils.StringUtils;
import com.zaxxer.hikari.pool.HikariPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author wangjj
 * @date 2017/8/8 20:47
 * @description 执行函数
 * @copyright(c) chinacscs all rights reserved
 */
public class ExecuteFunction {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteFunction.class);
    private static String logPath = System.getProperty("log.dir") + "/Process.log";
    private static IAlert alert;
    private static Integer clientId;
    private static Integer subscribeId;

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new RuntimeException("参数必须为2个！");
        }
        String func = args[0];
        String env = args[1];

        String driverMds = PropUtils.getString("mds.jdbc.driver");
        String dburlMds = PropUtils.getString("mds.jdbc.url");
        String userMds = PropUtils.getString("mds.jdbc.user");
        String passwordMds = PropUtils.getString("mds.jdbc.password");
        String driverStg = PropUtils.getString("stage.jdbc.driver");
        String dburlStg = PropUtils.getString("stage.jdbc.url");
        String userStg = PropUtils.getString("stage.jdbc.user");
        String passwordStg = PropUtils.getString("stage.jdbc.password");
        String driverTgt = PropUtils.getString("target.jdbc.driver");
        String dburlTgt = PropUtils.getString("target.jdbc.url");
        String userTgt = PropUtils.getString("target.jdbc.user");
        String passwordTgt = PropUtils.getString("target.jdbc.password");
        String databaseType = dburlStg.split(":")[1];
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

            JdbcManager jdbcManagerMds = null;
            try {
                jdbcManagerMds = new JdbcManager(dburlMds, userMds, passwordMds, driverMds);
            } catch (HikariPool.PoolInitializationException e) {
                logger.error("MDS数据库连接建立失败！");
                LogUtils.logException(logger, e);
                try {
                    String subject = "[S" + subscribeId + "C" + clientId + "]MDS数据库连接失败！";
                    String message = "请查看附件中的日志获取详细信息。。";
                    alert.sendAlert(subject, message, Arrays.asList(logPath), null);
                } catch (Exception e1) {
                    logger.error("报警发送失败！");
                    LogUtils.logException(logger, e1);
                    System.exit(-1);
                }
                System.exit(-1);
            }
            JdbcManager jdbcManagerStg = null;
            try {
                jdbcManagerStg = new JdbcManager(dburlStg, userStg, passwordStg, driverStg);
            } catch (HikariPool.PoolInitializationException e) {
                logger.error("Staging数据库连接建立失败！");
                LogUtils.logException(logger, e);
                try {
                    String subject = "[S" + subscribeId + "C" + clientId + "]staging数据库连接失败！";
                    String message = "请查看附件中的日志获取详细信息。。";
                    alert.sendAlert(subject, message, Arrays.asList(logPath), null);
                } catch (Exception e1) {
                    logger.error("报警发送失败！");
                    LogUtils.logException(logger, e1);
                    System.exit(-1);
                }
                System.exit(-1);
            }
            JdbcManager jdbcManagerTgt = null;
            try {
                if (!StringUtils.isEmpty(driverTgt)) {
                    jdbcManagerTgt = new JdbcManager(dburlTgt, userTgt, passwordTgt, driverTgt);
                }
            } catch (HikariPool.PoolInitializationException e) {
                logger.error("target数据库连接建立失败！");
                LogUtils.logException(logger, e);
                try {
                    String subject = "[S" + subscribeId + "C" + clientId + "]应用数据库连接失败！";
                    String message = "请查看附件中的日志获取详细信息。。";
                    alert.sendAlert(subject, message, Arrays.asList(logPath), null);
                } catch (Exception e1) {
                    logger.error("报警发送失败！");
                    LogUtils.logException(logger, e1);
                    System.exit(-1);
                }
                System.exit(-1);
            }

            String clientName = PropUtils.getString("login.username");
            String clientPassword = PropUtils.getString("login.password");
            String loginSql = "select client_id, subscribe_id from vw_client_basicinfo_" + clientName + " where client_snm = ? and client_pw = md5(md5(CONCAT(?,?))) and isdel = 0";
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

            String sql = "";
            if (Constants.DB_TYPE_ORACLE.equals(databaseType)) {
                sql = "select " + func + " from dual";
                logger.info("开始在" + env + "环境执行" + func);
                if (env.trim().toLowerCase().equals(Constants.ENV_STG)) {
                    Object result = jdbcManagerStg.executeQuery(sql, rs -> {
                        rs.next();
                        return rs.getObject(1);
                    }, null, true);
                    logger.info(func + "的执行结果是：" + result);
                } else if (env.trim().toLowerCase().equals(Constants.ENV_TGT) && (!StringUtils.isEmpty(driverTgt))) {
                    if (jdbcManagerTgt != null) {
                        Object result = jdbcManagerTgt.executeQuery(sql, rs -> {
                            rs.next();
                            return rs.getObject(1);
                        }, null, true);
                        logger.info(func + "的执行结果是：" + result);
                    }
                } else {
                    logger.error("错误的执行环境！");
                    System.exit(-1);
                }
            } else if (Constants.DB_TYPE_POSTGRESQL.equals(databaseType)) {
                sql = "{call " + func + "}";
                logger.info("开始在" + env + "环境执行" + func);
                if (env.trim().toLowerCase().equals(Constants.ENV_STG)) {
                    jdbcManagerStg.call(sql, null, true, true);
                } else if (env.trim().toLowerCase().equals(Constants.ENV_TGT)) {
                    if (jdbcManagerTgt != null) {
                        jdbcManagerTgt.call(sql, null, true, true);
                    }
                } else {
                    logger.error("错误的执行环境！");
                    System.exit(-1);
                }
            }
            logger.info(func + "执行完毕");
        } catch (ClassNotFoundException e) {
            logger.error("找不到JDBC驱动！");
            LogUtils.logException(logger, e);
            System.exit(2);
        } catch (SQLException e) {
            logger.error(func + "执行失败！");
            LogUtils.logException(logger, e);
            try {
                String subject = "[S" + subscribeId + "C" + clientId + "]在" + env + "执行" + func + "失败！";
                String message = "请查看附件中的日志获取详细信息。。";
                alert.sendAlert(subject, message, Arrays.asList(logPath), null);
                logger.error(subject);
            } catch (Exception e1) {
                logger.error("报警发送失败！");
                LogUtils.logException(logger, e1);
                System.exit(-1);
            }
            System.exit(-1);
        }
    }
}
