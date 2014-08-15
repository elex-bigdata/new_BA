package com.xingcloud.nba.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by Administrator on 14-8-1.
 */
public class DateManager {
    /**
     *
     * @param n 几天前日期
     * @param type 0：2014-07-29 1：20140729
     * @return
     */
    public static String getDaysBefore(int n, int type) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -n);
        SimpleDateFormat sdf = null;
        if(type == 0) {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        } else {
            sdf = new SimpleDateFormat("yyyyMMdd");
        }

        return sdf.format(cal.getTime());
    }

    /**
     * 日期转为时间戳字符串，如2014-08-11转成1407686400000
     * @param year ：2014
     * @param month ：7 （0开始）
     * @param day ：11
     * @return
     */
    public static long dateToTimestamp(int year, int month, int day) {
        Calendar cal = Calendar.getInstance();
        cal.set(year, month, day);
        long time = cal.getTimeInMillis();
        return time;
    }

    /**
     *
     * @param date 时间字符串，格式：2014-08-11
     * @return
     */
    public static long dateToTimestampString(String date) {
        String[] time = date.split("-");
        int year = Integer.parseInt(time[0]);
        int month = Integer.parseInt(time[1]) - 1;
        int day = Integer.parseInt(time[2]);
        Calendar cal = Calendar.getInstance();
        cal.set(year, month, day);
        return cal.getTimeInMillis();
    }
}
