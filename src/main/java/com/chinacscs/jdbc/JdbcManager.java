package com.chinacscs.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author wangjj
 * @date 2016/9/22 16:44
 * @description JDBC工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class JdbcManager {
    private String dburl;
    private String user;
    private String password;

    public ThreadLocal tl = new ThreadLocal();
    public HikariDataSource dataSource;

    /**
     * 构造函数
     *
     * @param dburl    JDBC连接地址
     * @param user     数据库用户名
     * @param password 数据库密码
     * @param driver   数据库驱动类
     * @throws ClassNotFoundException 找不到输入的数据库驱动类
     */
    public JdbcManager(String dburl, String user, String password, String driver) throws ClassNotFoundException {
        Class.forName(driver);
        this.dburl = dburl;
        this.user = user;
        this.password = password;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dburl);
        config.setUsername(user);
        config.setPassword(password);
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setAutoCommit(false);
        dataSource = new HikariDataSource(config);
    }

    /**
     * 获取数据库连接对象
     *
     * @return 数据库连接对象
     */
    public Connection getConnection() {
        try {
            return dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 调用数据库中的过程
     *
     * @param sql         调用过程的SQL语句
     * @param params      存有过程参数的数组
     * @param releaseConn 是否释放连接
     * @param autoCommit  是否自动提交
     * @return 过程影响的行数
     * @throws Exception 抛出异常
     */
    public int call(String sql, Object[] params, boolean releaseConn, boolean autoCommit) throws SQLException {
        int rtn = 0;
        PreparedStatement stmt = null;
        Connection conn = null;

        try {
            if (tl.get() == null) {
                conn = getConnection();
                tl.set(conn);
            } else {
                conn = (Connection) tl.get();
            }
            stmt = conn.prepareCall(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
            }
            rtn = stmt.executeUpdate();
            if (autoCommit) {
                conn.commit();
            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    throw new RuntimeException(e1);
                }
            }
            throw new SQLException(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (conn != null && releaseConn) {
                    close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return rtn;
    }

    /**
     * 执行DML语句
     *
     * @param sql         DML语句的SQL
     * @param params      DML语句的参数所在数组
     * @param releaseConn 是否释放连接
     * @return DML语句影响的的行数
     * @throws Exception 抛出异常
     */
    public int executeUpdate(String sql, Object[] params, boolean releaseConn, boolean autoCommit) throws SQLException {
        int rtn = 0;
        PreparedStatement stmt = null;
        Connection conn = null;

        try {
            if (tl.get() == null) {
                conn = getConnection();
                tl.set(conn);
            } else {
                conn = (Connection) tl.get();
            }
            stmt = conn.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
            }
            rtn = stmt.executeUpdate();
            if (autoCommit) {
                conn.commit();
            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    throw new RuntimeException(e1);
                }
            }
            throw new SQLException(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (conn != null && releaseConn) {
                    close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return rtn;
    }

    /**
     * 执行查询语句
     *
     * @param sql           查询语句的SQL
     * @param resultHandler 查询结果集的处理句柄
     * @param params        查询语句的参数所在的数组
     * @param releaseConn   是否释放连接
     * @return 查询结果集封装的对象
     * @throws Exception 抛出异常
     */
    public Object executeQuery(String sql, ResultHandler resultHandler, Object[] params, boolean releaseConn)
            throws SQLException {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = null;

        try {
            if (tl.get() == null) {
                conn = getConnection();
                tl.set(conn);
            } else {
                conn = (Connection) tl.get();
            }
            stmt = conn.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
            }
            rs = stmt.executeQuery();
            return resultHandler.handle(rs);
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (conn != null && releaseConn) {
                    close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 批量执行DML语句
     *
     * @param sql         DML语句的SQL
     * @param paramList   多个DML语句的参数所在的数组构成的List
     * @param releaseConn 是否释放连接
     * @return 每个DML语句影响的的行数构成的行数
     * @throws Exception 抛出异常
     */
    public int[] executeBatch(String sql, List<Object[]> paramList, boolean releaseConn, boolean autoCommit) throws SQLException {
        int[] rtn = null;
        PreparedStatement stmt = null;
        Connection conn = null;

        try {
            if (tl.get() == null) {
                conn = getConnection();
                tl.set(conn);
            } else {
                conn = (Connection) tl.get();
            }
            stmt = conn.prepareStatement(sql);
            if (paramList != null) {
                for (Object[] params : paramList) {
                    for (int i = 0; i < params.length; i++) {
                        stmt.setObject(i + 1, params[i]);
                    }
                    stmt.addBatch();
                }
            }
            rtn = stmt.executeBatch();
            if (autoCommit) {
                conn.commit();
            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    throw new RuntimeException(e1);
                }
            }
            throw new SQLException(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (conn != null && releaseConn) {
                    close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return rtn;
    }

    public void close() throws SQLException {
        Connection conn = (Connection) tl.get();
        if (conn != null) {
            conn.close();
        }
        tl.set(null);
        tl.remove();
    }

    public void commit() throws SQLException {
        Connection conn = (Connection) tl.get();
        if (conn != null) {
            conn.commit();
        }
    }

    public void rollback() throws SQLException {
        Connection conn = (Connection) tl.get();
        if (conn != null) {
            conn.rollback();
        }
    }

    /**
     * 批量执行DML语句
     * 可以指定batchSize, 进行分批提交
     *
     * @param sql         DML语句的SQL
     * @param paramList   多个DML语句的参数所在的数组构成的List
     * @param releaseConn 是否释放连接
     * @return 每个DML语句影响的的行数构成的行数
     * @throws SQLException 抛出异常
     */
    public int executeBatchWithBatchSize(String sql, List<Object[]> paramList, boolean releaseConn, boolean autoCommit, Integer batchSize) throws SQLException {
        int totalCount = 0;
        int[] rtn = null;
        PreparedStatement stmt = null;
        Connection conn = null;

        try {
            if (tl.get() == null) {
                conn = getConnection();
                tl.set(conn);
            } else {
                conn = (Connection) tl.get();
            }
            stmt = conn.prepareStatement(sql);
            if (paramList != null) {
                int dataSize = paramList.size();
                for (int j = 0; j < dataSize; j++) {
                    Object[] params = paramList.get(j);
                    for (int i = 0; i < params.length; i++) {
                        stmt.setObject(i + 1, params[i]);
                    }
                    stmt.addBatch();
                    //每batchSize条数据执行一次executeBatch
                    if (batchSize != null && batchSize > 0 && j % batchSize == 0) {
                        rtn = stmt.executeBatch();
                        totalCount += rtn.length;
                    }
                }
            }
            rtn = stmt.executeBatch();
            totalCount += rtn.length;
            if (autoCommit) {
                conn.commit();
            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                    throw new RuntimeException(e1);
                }
            }
            throw new SQLException(e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (conn != null && releaseConn) {
                    close();
                    conn = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return totalCount;
    }
}
