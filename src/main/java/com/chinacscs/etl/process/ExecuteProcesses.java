package com.chinacscs.etl.process;

import com.chinacscs.utils.LogUtils;
import com.chinacscs.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangjj
 * @date 2017/8/9 20:45
 * @description 函数或者存储过程执行调度
 * @copyright(c) chinacscs all rights reserved
 */
public class ExecuteProcesses {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteProcesses.class);
    final static String PROCESS_LIST_FILE_NAME = "processes.properties";
    static Properties processes = new Properties();
    static {
        try {
            ClassLoader cl = StringUtils.class.getClassLoader();
            InputStream in = cl.getResourceAsStream(PROCESS_LIST_FILE_NAME);
            processes.load(in);
        } catch(Exception e) {
            logger.warn(PROCESS_LIST_FILE_NAME + "文件不存在，取消process执行！");
            LogUtils.logException(logger, e);
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        for (Map.Entry process : processes.entrySet()) {
            String processName = process.getKey().toString().trim().toLowerCase();
            String env = process.getValue().toString().trim().toLowerCase();
            if(processName.startsWith("sp")) {
                ExecuteProcedure.main(new String[]{processName, env});
            } else if(processName.startsWith("fn")) {
                ExecuteFunction.main(new String[]{processName, env});
            }
        }
    }
}
