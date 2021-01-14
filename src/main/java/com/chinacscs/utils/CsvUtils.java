package com.chinacscs.utils;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author wangjj
 * @date 2016/9/29 15:21
 * @description CSV处理工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class CsvUtils {
    /**
     * CSV行处理句柄
     */
    @FunctionalInterface
    public interface CsvHandler {
        /**
         * CSV行处理方法
         *
         * @param reader
         * @throws IOException
         */
        void handle(CsvReader reader) throws IOException;
    }

    /**
     * 读取CSV文件，并对header进行清理
     *
     * @param path      CSV文件路径
     * @param delimiter CSV分隔符
     * @param decode    CSV编码
     * @return 返回读取到的CSV文件
     * @throws IOException 文件读取异常
     */
    public static CsvReader getCSVReader(String path, char delimiter, String decode, boolean withHeader) throws IOException {
        CsvReader reader = new CsvReader(path, delimiter, Charset.forName(decode));
        reader.setSafetySwitch(false);
        if (withHeader) {
            reader.readHeaders();
            String[] newHeaders = new String[reader.getHeaderCount()];
            int i = 0;
            for (String header : reader.getHeaders()) {
                //清洗掉header中的空格和单引号
                newHeaders[i] = header.replaceAll(" ", "").replaceAll("'", "");
                i++;
            }
            reader.setHeaders(newHeaders);
        }
        return reader;
    }

    /**
     * 循环遍历CSV中的行
     *
     * @param reader     CsvReader
     * @param csvHandler CSV处理句柄
     * @throws IOException
     */
    public static void traverseCSV(CsvReader reader, CsvHandler csvHandler) throws IOException {
        while (reader.readRecord()) {
            csvHandler.handle(reader);
        }
    }

    public static class CsvFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.toLowerCase().matches("^stg.*csv$");
        }
    }

    public static void writeCsv(String savePath, char delimiter, String encoding, List<String[]> contents) throws IOException {
        CsvWriter csvWriter = new CsvWriter(savePath, delimiter, Charset.forName(encoding));
        for (String[] content : contents) {
            csvWriter.writeRecord(content);
        }
        csvWriter.close();
    }

    public static Integer getRowCount(String path, char delimiter, String decode, boolean withHeader) throws IOException {
        CsvReader csvReader = CsvUtils.getCSVReader(path, delimiter, decode, withHeader);
        int[] origRecordCount = {0};
        CsvUtils.traverseCSV(csvReader, reader1 -> origRecordCount[0]++);
        csvReader.close();
        return origRecordCount[0];
    }
}
