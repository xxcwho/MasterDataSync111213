package com.chinacscs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author wangjj
 * @date 2016/9/22 15:20
 * @description 结果集处理句柄
 * @copyright(c) chinacscs all rights reserved
 */
@FunctionalInterface
public interface ResultHandler {

    /**
     * 结果集处理方法
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    Object handle(ResultSet rs) throws SQLException;
}
