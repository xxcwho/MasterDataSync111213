package com.chinacscs.params;

import java.util.HashSet;
import java.util.Set;

/**
 * @author flackyang
 * @date 2017/12/28 15:32
 * @description 常量维护类
 * @copyright(c) chinacscs all rights reserved
 */
public class Constants {

    /** 数据库支持列表 */
    public static final Set<String> SUPPORTED_DATABASE = new HashSet<String>() {{
        add("oracle");
        add("postgresql");
        add("mysql");
    }};

    /** 数据库类型_oracle */
    public static final String DB_TYPE_ORACLE = "oracle";

    /** 数据库类型_postgresql */
    public static final String DB_TYPE_POSTGRESQL = "postgresql";

    /** 数据库类型_mysql */
    public static final String DB_TYPE_MYSQL = "mysql";

    /** 执行环境_stg */
    public static final String ENV_STG = "stg";

    /** 执行环境_tgt */
    public static final String ENV_TGT = "tgt";

    /** 默认的MDS分发视图 */
    public static final String DEFAULT_SUBSCRIBE_VIEW = "VW_SUBSCRIBE_TABLE";

    /** 数据同步步骤_只从STFP下载数据文件 */
    public static final int SYNC_STEP_DOWNLOADFILE = 1;
    /** 数据同步步骤_下载数据文件并加载到数据库 */
    public static final int SYNC_STEP_LOADTODB = 2;

    /** 数据同步类型_原MDS方式 */
    public static final int SYNC_TYPE_MDS = 1;
    /** 数据同步类型_通用数据文件方式 */
    public static final int SYNC_TYPE_GDS = 2;
    /** 数据同步类型_客户数据输入 */
    public static final int SYNC_TYPE_CLIENT = 3;
}
