package com.chinacscs.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangjj
 * @date 2016/9/22 15:19
 * @description 结果集处理句柄的实现，对结果集进行封装
 * @copyright(c) chinacscs all rights reserved
 */
public class ListResultHandler implements ResultHandler {
    /**
     * 将结果集封装到一个List中
     * @param rs 结果集对象
     * @return 处理结果的封装
     * @throws Exception 抛出异常
     */
    @Override
    public Object handle(ResultSet rs) throws SQLException {
        List<List<Object>> rowList = new ArrayList<>();
        while (rs.next()) {
            List<Object> dataList = new ArrayList<>();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            Object columnValue;

            for (int i = 1; i <= columnCount; i++) {
                columnValue = rs.getObject(i);
                dataList.add(columnValue);
            }
            rowList.add(dataList);
        }
        return rowList;
    }
}
