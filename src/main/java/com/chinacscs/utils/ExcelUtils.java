package com.chinacscs.utils;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;

import java.io.*;

/**
 * @author wangjj
 * @date 2016/9/22 17:32
 * @description Excel工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class ExcelUtils {
    /**
     * Excel行处理句柄接口
     */
    @FunctionalInterface
    public interface RowHandler {
        /**
         * 自定义对Excel中某个Sheet的一行的处理动作
         *
         * @param row         Excel中某个Sheet的一行
         * @param startColNum Excel中某个Sheet的一行的遍历起始列号
         * @throws Exception 抛出异常
         */
        public void handle(Row row, int startColNum) throws Exception;
    }

    /**
     * Excel行遍历打印句柄
     */
    public static class RowPrintHandler implements RowHandler {
        @Override
        public void handle(Row row, int startColNum) {
            for (int i = startColNum; i <= row.getLastCellNum(); i++) {
                Cell cell = row.getCell(i);
                if (cell != null) {
                    System.out.println(cell.getColumnIndex() + ":" + cell.toString());
                }
            }
        }
    }

    /**
     * 根据Excel的路径和Sheet的名字读取Sheet
     *
     * @param path      Excel的路径
     * @param sheetName Sheet的名字
     * @return 读取到的Sheet对象
     * @throws IOException            抛出异常
     * @throws InvalidFormatException 抛出异常
     */
    public static Sheet readExcel(String path, String sheetName) throws IOException, InvalidFormatException {
        InputStream in = new FileInputStream(path);
        Workbook wb = WorkbookFactory.create(in);
        Sheet sheet = wb.getSheet(sheetName);
        return sheet;
    }

    /**
     * 根据Excel的路径和Sheet的编号读取Sheet
     *
     * @param path     Excel的路径
     * @param sheetNum Sheet的编号
     * @return 读取到的Sheet对象
     * @throws IOException            抛出异常
     * @throws InvalidFormatException 抛出异常
     */
    public static Sheet readExcel(String path, int sheetNum) throws IOException, InvalidFormatException {
        InputStream in = new FileInputStream(path);
        Workbook wb = WorkbookFactory.create(in);
        Sheet sheet = wb.getSheetAt(sheetNum);
        return sheet;
    }

    /**
     * 遍历Sheet
     *
     * @param sheet       需要遍历的Sheet对象
     * @param startRowNum 需要遍历的Sheet的起始行号
     * @param endRowNum   需要遍历的Sheet的结束行号
     * @param startColNum 需要遍历的Sheet的起始列号
     * @param wbHandler   行处理句柄
     * @throws Exception 抛出异常
     */
    public static void traverseWb(Sheet sheet, int startRowNum, int endRowNum, int startColNum, RowHandler wbHandler) throws Exception {
        for (int i = startRowNum; i <= endRowNum; i++) {
            Row row = sheet.getRow(i);
            wbHandler.handle(row, startColNum);
        }
    }

    /**
     * 将Cell中的数据读取成一个String
     *
     * @param row    Cell所在行对象
     * @param colNum Cell所在列的编号
     * @return 读取到的String类型数据
     */
    public static String getStringValueFromColNum(Row row, Integer colNum) {
        try {
            if (colNum == null) {
                return null;
            }
            Cell cell = row.getCell(colNum);
            if (cell == null) {
                return null;
            } else {
                if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
                    try {
                        return cell.getNumericCellValue() + "";
                    } catch (Exception e) {
                        return cell.toString().trim();
                    }
                } else {
                    return cell.toString().trim();
                }
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 将Cell中的数据读取成一个Integer
     *
     * @param row    Cell所在行对象
     * @param colNum Cell所在列的编号
     * @return 读取到的Integer类型数据
     */
    public static Integer getIntegerValueFromColNum(Row row, Integer colNum) {
        try {
            if (colNum == null) {
                return null;
            }
            Cell cell = row.getCell(colNum);
            if (cell == null) {
                return null;
            } else {
                if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC || cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
                    return (int) cell.getNumericCellValue();
                } else {
                    return Integer.parseInt(cell.toString());
                }
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 文件名过滤器，只取xlsx格式且不以~$打头的文件
     */
    public static class ExcelFileFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(".xlsx") && (!name.startsWith("~$"));
        }
    }
}
