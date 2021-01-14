package com.chinacscs.utils;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author wangjj
 * @date 2017/3/13 17:32
 * @description FTP工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class FtpUtils {
    private static final Logger logger = LoggerFactory.getLogger(FtpUtils.class);
    public static FTPClient login(String username, String password, String host, Integer port) throws IOException {
        FTPClient ftpClient = new FTPClient();
        ftpClient.connect(host, port);
        ftpClient.login(username, password);

        ftpClient.setBufferSize(8192);
        ftpClient.setDataTimeout(30 * 1000);
        ftpClient.setFileTransferMode(FTPClient.BINARY_FILE_TYPE);
        ftpClient.setAutodetectUTF8(true);

        return ftpClient;
    }

    public static void logout(FTPClient ftpClient) throws IOException {
        try {
            ftpClient.logout();
        } finally {
            ftpClient.disconnect();
        }
    }

    private static String directoryPathCleaner(String directory) {
        if(!directory.endsWith("/")) {
            return directory + "/";
        } else {
            return  directory;
        }
    }

    public static void downloadFile(FTPClient ftpClient, String fileName, String remoteDirectory, String localDirectory) throws IOException {
        remoteDirectory = directoryPathCleaner(remoteDirectory);
        localDirectory = directoryPathCleaner(localDirectory);

        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(localDirectory + fileName))) {
            boolean success = ftpClient.retrieveFile(remoteDirectory + fileName, bufferedOutputStream);
            bufferedOutputStream.flush();
            if(!success) {
                throw new IOException();
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public static void uploadFile(FTPClient ftpClient, String fileName, String remoteDirectory, String localDirectory) throws IOException {
        remoteDirectory = directoryPathCleaner(remoteDirectory);
        localDirectory = directoryPathCleaner(localDirectory);

        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(localDirectory + fileName))) {
            ftpClient.changeWorkingDirectory(remoteDirectory);

            boolean success = ftpClient.storeFile(remoteDirectory + fileName, bufferedInputStream);
            if(!success) {
                throw new IOException();
            }
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public static void deleteFile(FTPClient ftpClient, String fileName, String directory) throws IOException {
        directory = directoryPathCleaner(directory);
        boolean success = ftpClient.deleteFile(directory + fileName);
        if(!success) {
            throw new IOException();
        }
    }

    public static long getFileSize(FTPClient ftpClient, String filePath) throws IOException {
        FTPFile[] ftpFiles = ftpClient.listFiles(filePath);
        if(ftpFiles.length == 0) {
            throw new IOException();
        } else {
            return ftpFiles[0].getSize();
        }
    }

    public static long getFileSize(FTPClient ftpClient, String fileName, String directory) throws IOException {
        directory = directoryPathCleaner(directory);
        return  getFileSize(ftpClient, directory + fileName);
    }
}
