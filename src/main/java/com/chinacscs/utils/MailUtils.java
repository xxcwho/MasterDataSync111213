package com.chinacscs.utils;

import org.apache.commons.mail.EmailAttachment;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.MultiPartEmail;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangjj
 * @date 2016/12/27 15:54
 * @description 邮件工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class MailUtils {
    public static void sendMail(Map<String, Object> mailProperties) throws EmailException {
        MultiPartEmail email = new MultiPartEmail();
        //设置收件人
        if(mailProperties.containsKey("mailToList")) {
            String mailToList = (String) mailProperties.get("mailToList");
            for (String mailTo : mailToList.split(",")) {
                email.addTo(mailTo);
            }
        } else {
            throw new EmailException("收件人不能为空！");
        }
        //设置抄送人
        if(mailProperties.containsKey("mailCcList") && (!StringUtils.isEmpty((String)mailProperties.get("mailCcList")))) {
            String mailCcList = (String) mailProperties.get("mailCcList");
            for (String mailCc : mailCcList.split(",")) {
                email.addCc(mailCc);
            }
        }
        //设置暗抄人
        if(mailProperties.containsKey("mailBccList")) {
            String mailBccList = (String) mailProperties.get("mailBccList");
            for (String mailBcc : mailBccList.split(",")) {
                email.addBcc(mailBcc);
            }
        }
        //设置附件
        if(mailProperties.containsKey("mailAttachmentList") && mailProperties.get("mailAttachmentList") != null) {
            List<String> mailAttachmentList = (List<String>) mailProperties.get("mailAttachmentList");
            for (String mailAttachment : mailAttachmentList) {
                EmailAttachment attachment = new EmailAttachment();
                attachment.setDisposition(EmailAttachment.ATTACHMENT);
                attachment.setPath(mailAttachment);
                email.attach(attachment);
            }
        }
        //设置发件人服务器，用户名密码
        if(mailProperties.containsKey("senderHost")) {
            String senderHost = (String) mailProperties.get("senderHost");
            email.setHostName(senderHost);
        } else {
            throw new EmailException("发件人服务器名不能为空！");
        }
        if(mailProperties.containsKey("senderUser") && mailProperties.containsKey("senderPassword")) {
            String senderUser = (String) mailProperties.get("senderUser");
            String senderPassword = (String) mailProperties.get("senderPassword");
            email.setAuthentication(senderUser, senderPassword);
            email.setFrom(senderUser);
        } else {
            throw new EmailException("发件人用户名密码不能为空！");
        }
        //设置发件服务器端口
        if(mailProperties.containsKey("senderPort") && (!StringUtils.isEmpty((String)mailProperties.get("senderPort")))) {
            email.setSmtpPort((Integer)mailProperties.get("senderPort"));
        }
        //设置邮件主题
        if(mailProperties.containsKey("mailSubject")) {
            String mailSubject = (String) mailProperties.get("mailSubject");
            email.setSubject(mailSubject);
        }
        //设置邮件正文
        if(mailProperties.containsKey("mailMessage")) {
            String mailMessage = (String) mailProperties.get("mailMessage");
            email.setMsg(mailMessage);
        }

        //发送邮件
        email.send();
    }

    public static void sendMailWithoutDomain(Map<String, Object> mailProperties) throws MessagingException, UnsupportedEncodingException {
        Session session; // 邮件会话对象
        MimeMessage mimeMsg; // MIME邮件对象
        Multipart mp;   // Multipart对象,邮件内容,标题,附件等内容均添加到其中后再生成MimeMessage对象


        //读取参数
        String authType = (String)mailProperties.get("AuthType");
        String senderHost = (String)mailProperties.get("senderHost");
        String senderPort = (String)mailProperties.get("senderPort");
        String senderUser = (String)mailProperties.get("senderUser");
        String senderPassword = (String)mailProperties.get("senderPassword");
        String mailToList = (String)mailProperties.get("mailToList");
        String mailCcList = (String)mailProperties.get("mailCcList");
        String mailSubject = (String)mailProperties.get("mailSubject");
        String mailMessage = (String)mailProperties.get("mailMessage");
        List<String> mailAttachmentList = (List<String>)mailProperties.get("mailAttachmentList");


        // 建立会话
        Properties props;
        props = System.getProperties();
        if(StringUtils.isEmpty(authType)) {
            props.put("mail.smtp.auth", "true");
        } else {
            props.put("mail.smtp.auth", authType);
        }
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.host", senderHost);
        if(StringUtils.isEmpty(senderPort)) {
            props.put("mail.smtp.port", "25");
        } else {
            props.put("mail.smtp.port", senderPort);
        }
        props.put("username", senderUser);
        props.put("password", senderPassword);
        session = Session.getDefaultInstance(props);
        session.setDebug(false);

        mimeMsg = new MimeMessage(session);
        mp = new MimeMultipart();

        // 设置发件人
        String from = senderUser;
        senderUser = senderUser.split("@")[0];//截掉domain
        mimeMsg.setFrom(new InternetAddress(from));

        // 设置收件人
        mimeMsg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mailToList));
        // 设置抄送人
        if (!StringUtils.isEmpty(mailCcList)) {
            mimeMsg.setRecipients(Message.RecipientType.CC, InternetAddress.parse(mailCcList));
        }
        // 设置主题
        mimeMsg.setSubject(mailSubject);
        // 设置正文
        BodyPart bp = new MimeBodyPart();
        bp.setContent(mailMessage, "text/html;charset=utf-8");
        mp.addBodyPart(bp);

        // 设置附件
        if (mailAttachmentList != null && mailAttachmentList.size() > 0) {
            for (int i = 0; i < mailAttachmentList.size(); i++) {
                bp = new MimeBodyPart();
                FileDataSource fds = new FileDataSource(mailAttachmentList.get(i));
                bp.setDataHandler(new DataHandler(fds));
                bp.setFileName(MimeUtility.encodeText(fds.getName(), "UTF-8", "B"));
                mp.addBodyPart(bp);
            }
        }
        mimeMsg.setContent(mp);
        mimeMsg.saveChanges();

        // 发送邮件
        Transport transport = session.getTransport("smtp");
        transport.connect(senderHost, senderUser, senderPassword);
        transport.sendMessage(mimeMsg, mimeMsg.getAllRecipients());
        transport.close();
    }
}
