package com.xingcloud.nba.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 14-8-1.
 */
public class DateManager {

    public static SimpleDateFormat dayfmt = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat dayfmt2 = new SimpleDateFormat("yyyyMMdd");

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

    public static String getDaysBefore(Date day, int n, int type) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(day);
        cal.add(Calendar.DATE, -n);
        SimpleDateFormat sdf = null;
        if(type == 0) {
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        } else {
            sdf = new SimpleDateFormat("yyyyMMdd");
        }

        return sdf.format(cal.getTime());
    }

    public static String getDaysBefore(String day, int n) throws Exception{
        Date date = dayfmt.parse(day);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, -n);

        return dayfmt.format(cal.getTime());
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
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    public static Long[] dayStartEnd(String date) throws ParseException {
        Date day = dayfmt.parse(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(day);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long start = cal.getTimeInMillis();

        cal.set(Calendar.HOUR_OF_DAY, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);
        long end = cal.getTimeInMillis();

        return new Long[]{start,end};
    }

    public static void main(String[] args) throws Exception{
//        String day = "20141207";
//        String end = day;
//        day = day.substring(0,4) + "-" + day.substring(4,6) + "-" + day.substring(6);
//        System.out.println(day);
//        System.out.println(end);

//        System.out.println(getDaysBefore(day, 1));
//        Date d = new Date();
//        String d1 = dayfmt.format(d);
//        System.out.println(getDaysBefore(d1, 1));


        String day = "2014-12-18";
        String yesterdayKey = day + " 00:00";
        String scanDay = DateManager.getDaysBefore(day, 1);
        String valueKey = scanDay + " 00:00";
        String date = scanDay.replace("-","");
        String start = DateManager.getDaysBefore(day, 6);
        String end = DateManager.dayfmt.format(DateManager.dayfmt.parse(day));
        System.out.println(yesterdayKey);
        System.out.println(scanDay);
        System.out.println(date);
        System.out.println(start);
        System.out.println(end);

        Long[] tt = dayStartEnd("2015-02-02");
        System.out.println(tt[0]);
        System.out.println(tt[1]);
    }
}
