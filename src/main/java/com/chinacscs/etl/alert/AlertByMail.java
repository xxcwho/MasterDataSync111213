package com.chinacscs.etl.alert;

import com.chinacscs.utils.MailUtils;
import com.chinacscs.utils.PropUtils;
import com.chinacscs.utils.StringUtils;
import org.apache.commons.mail.EmailException;

import javax.mail.MessagingException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;

/**
 * @author wangjj
 * @date 2017/8/11 20:50
 * @description 通过邮件发送告警
 * @copyright(c) chinacscs all rights reserved
 */
public class AlertByMail implements IAlert{
    private String mailToList;
    private String mailCcList;
    private String senderHost;
    private String senderPort;
    private String authType;
    private String senderUser;
    private String senderPassword;
    private Boolean withoutDomain;

    private AlertByMail() {
        mailToList = PropUtils.getString("mail.to");
        mailCcList = PropUtils.getString("mail.cc");
        senderHost = PropUtils.getString("mail.senderHost");
        senderPort = PropUtils.getString("mail.senderPort");
        senderUser = PropUtils.getString("mail.senderUser");
        authType = PropUtils.getString("mail.authType");
        senderPassword = PropUtils.getString("mail.senderPassword");
        withoutDomain = PropUtils.getBoolean("mail.withoutDomain");
    }

    @Override
    public void sendAlert(String mailSubject, String mailMessage, List<String> mailAttachmentList, String alertLevel) throws UnsupportedEncodingException, MessagingException, EmailException {
        if (StringUtils.isEmpty(mailToList)) {
            System.out.println("收件人为空，取消发送邮件...");
        } else {
            HashMap<String, Object> mailProperties = new HashMap<>(16);
            mailProperties.put("mailToList", mailToList);
            mailProperties.put("mailCcList", mailCcList);
            mailProperties.put("senderHost", senderHost);
            mailProperties.put("senderPort", senderPort);
            mailProperties.put("senderUser", senderUser);
            mailProperties.put("authType", authType);
            mailProperties.put("senderPassword", senderPassword);
            mailProperties.put("mailSubject", mailSubject);
            mailProperties.put("mailMessage", mailMessage);
            mailProperties.put("mailAttachmentList", mailAttachmentList);

            if(withoutDomain) {
                MailUtils.sendMailWithoutDomain(mailProperties);
            } else {
                MailUtils.sendMail(mailProperties);
            }
        }
    }
}
