package com.chinacscs.utils;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * @author wangjj
 * @date 2016/12/23 17:21
 * @description Sftp工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class SftpUtils {
    public static ChannelSftp getConnectionByPassword(String username, String password, String host, Integer port) throws JSchException {
        JSch jSch = new JSch();
        Session sshSession = jSch.getSession(username, host, port);
        sshSession.setPassword(password);
        sshSession.setConfig("StrictHostKeyChecking", "no");
        sshSession.connect();
        ChannelSftp sftp = (ChannelSftp) sshSession.openChannel("sftp");
        sftp.connect();
        return sftp;
    }

    public static ChannelSftp getConnectionByKey(String username, String prvkey, String host, Integer port) throws JSchException {
        JSch jSch = new JSch();
        jSch.addIdentity(prvkey);
        Session sshSession = jSch.getSession(username, host, port);
        sshSession.setConfig("StrictHostKeyChecking", "no");
        sshSession.connect();
        ChannelSftp sftp = (ChannelSftp) sshSession.openChannel("sftp");
        sftp.connect();
        return sftp;
    }

    public static void close(ChannelSftp sftp) throws JSchException {
        sftp.disconnect();
        sftp.getSession().disconnect();
    }
}
