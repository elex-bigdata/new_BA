package com.xingcloud.nba.utils;

/**
 * Created by Administrator on 14-7-31.
 */
public class Constant {
    public static int KEY_USELESS = 0;
    public static int KEY_FOR_EVENT_LOG = 1;
    public static int KEY_FOR_IDMAP = 2;
    public static int KEY_FOR_MYSQL = 3;

    public static int DAY_ACTIVE_COUNT = 1;     //日活跃
    public static int WEEK_ACTIVE_COUNT = 2;    //周活跃
    public static int MONTH_ACTIVE_COUNT = 3;   //月活跃

    public static int DAY_UNIQ = 0;     //当日uid去重
    public static int WEEK_UNIQ = 1;    //一周内uid去重

    public static int ACT_UNIQ = 0;     //日活跃去重
    public static int NEW_UNIQ = 1;    //每日新增用户去重

    public static int TWO_RET = 2;  //二日留存
    public static int SEVEN_RET = 7;  //七日留存

    public static final String INTERNET1 = "internet-1";
    public static final String INTERNET2 = "internet-2";
    public static final String INTERNET = "internet";

    public static final String SPECIAL_TASK_PATH = "/home/hadoop/ba/BA/conf/specialtask";
    public static final String HDFS_CATCH_PATH = "/hadoop/user/pj_hive_result/";

    public static final String NATION = "nation";
    public static final String EV3 = "event3";
    public static final String EV4 = "event4";
    public static final String EV5 = "event5";
    public static final String HDFS_SEARCH_PATH = "hdfs://ELEX-LA-WEB1:19000/hadoop/user/search/";

    public static final String EVENT = "pay.search2";

}
