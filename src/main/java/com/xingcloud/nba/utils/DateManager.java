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
}
