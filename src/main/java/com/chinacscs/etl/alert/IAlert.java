package com.chinacscs.etl.alert;

import java.util.List;

/**
 * @author wangjj
 * @date 2017/8/11 20:48
 * @description 告警接口
 * @copyright(c) chinacscs all rights reserved
 */
@FunctionalInterface
public interface IAlert {
    /**
     * 发送告警
     *
     * @param subject        告警标题
     * @param message        告警内容
     * @param attachmentList 告警附件
     * @param alertLevel     告警级别
     * @throws Exception
     */
    void sendAlert(String subject, String message, List<String> attachmentList, String alertLevel) throws Exception;
}
