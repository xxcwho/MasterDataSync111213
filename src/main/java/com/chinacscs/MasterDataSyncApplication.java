package com.chinacscs;

import com.chinacscs.etl.process.ExecuteProcesses;
import com.chinacscs.etl.sftp.DownloadDataFile;
import com.chinacscs.etl.sftp.DownloadFromSftp;
import com.chinacscs.etl.stage.StageLoad;
import com.chinacscs.etl.target.TargetLoad;
import com.chinacscs.params.Constants;
import com.chinacscs.utils.PropUtils;
import com.chinacscs.utils.StringUtils;
import org.apache.commons.mail.EmailException;

import java.util.Arrays;

/**
 * @author wangjj
 * @date 2016/12/30 20:51
 * @description 数据同步调度入口
 * @copyright(c) chinacscs all rights reserved
 */
public class MasterDataSyncApplication {
    public static void main(String[] args) throws EmailException {
        String taskName = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        if (StringUtils.isEmpty(taskName)) {
            throw new IllegalArgumentException("任务名不能为空！");
        } else if (taskName.toLowerCase().equals("help") || taskName.toLowerCase().equals("h") || taskName.toLowerCase().equals("--h") || taskName.toLowerCase().equals("-h") || taskName.toLowerCase().equals("--help") || taskName.toLowerCase().equals("-help")) {
            System.out.println("可选任务:\n" +
                    "\t\tDownloadFromSftp\n" +
                    "\t\tStageLoad\n" +
                    "\t\tTargetLoad\n" +
                    "\t\tExecuteProcesses\n"
            );
            System.exit(0);
        } else if (taskName.toLowerCase().equals("downloadfromsftp")) {
            Integer syncType = PropUtils.getOrDefault("sync.type", Constants.SYNC_TYPE_MDS);
            if (syncType.equals(Constants.SYNC_TYPE_MDS)) {
                DownloadFromSftp.main(args);
            } else if (syncType.equals(Constants.SYNC_TYPE_GDS)) {
                DownloadDataFile.main(args);
            } else {
                throw new RuntimeException("同步类型配置错误，未知的同步类型");
            }
        } else if (taskName.toLowerCase().equals("stageload")) {
            StageLoad.main(args);
        } else if (taskName.toLowerCase().equals("targetload")) {
            TargetLoad.main(args);
        } else if (taskName.toLowerCase().equals("executeprocesses")) {
            ExecuteProcesses.main(args);
        } else {
            throw new IllegalArgumentException("不存在该任务！可尝试help查看可选参数值");
        }
    }
}
