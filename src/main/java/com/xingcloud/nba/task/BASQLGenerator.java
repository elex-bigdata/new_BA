package com.xingcloud.nba.task;

import com.xingcloud.nba.utils.DateManager;

import java.text.ParseException;
import java.util.List;

/**
 * 组装一下比较复杂的SQL
 * Author: liqiang
 * Date: 14-8-27
 * Time: 上午9:51
 */
public class BASQLGenerator {

    public static String getTransVistUIDSql(String project, List<String> pids, String day) throws ParseException {

        Long[] dayStartEnd = DateManager.dayStartEnd(day);

        StringBuffer sb = new StringBuffer();
        sb.append("insert overwrite table user_visit partition(pid='").append(project).append("',day='").append(day).append("') ")
                .append("select distinct lower(ui.orig_id) from user_event ue join user_id ui on ue.uid = ui.uid and ue.pid = ui.pid ")
                .append(" where ue.day = '").append(day).append("' and ue.pid in ('").append(pids.get(0)).append("'");

        for(int i=1;i<pids.size();i++){
            sb.append(",'").append(pids.get(i)).append("'");
        }

        sb.append(") and (ue.event like 'visit.%' or ue.event like 'ientheartbeat.%')  and ue.ts >= '")
            .append(dayStartEnd[0]).append("' and ue.ts  <='").append(dayStartEnd[1]).append("' ");

        return sb.toString();
    }

    public static String getCombineVisitUIDSql(String project, String day, String[] projects){
        StringBuffer sb = new StringBuffer();
        sb.append("insert overwrite table user_visit  partition(pid='").append(project).append("',day='").append(day).append("') ")
                .append("select distinct orig_id from user_visit where pid in ('")
                .append(projects[0]).append("','").append(projects[1]).append("') and day='").append(day).append("'");
        return sb.toString();
    }


    public static String getTransRegisterTimeUIDSql(String project, List<String> pids){
        StringBuffer sb = new StringBuffer();
        sb.append("insert overwrite table user_register_time  partition(pid='").append(project).append("')")
                .append("select lower(ui.orig_id), substr(max(up.val),0,8), substr(min(up.val),0,8) from user_property up join user_id ui on up.uid = md5uid(ui.uid) and up.pid = ui.pid ")
                .append("where  up.prop = 'register_time' and up.pid in ('").append(pids.get(0)).append("'");

        for(int i=1;i<pids.size();i++){
            sb.append(",'").append(pids.get(i)).append("'");
        }
        sb.append(") group by lower(ui.orig_id)");

        return sb.toString();
    }

    public static String getCombineRegisterTimeUIDSql(String project, String[] projects){
        StringBuffer sb = new StringBuffer();
        sb.append("insert overwrite table user_register_time  partition(pid='").append(project).append("') ")
                .append("select orig_id,max(max_reg_time), min(min_reg_time) from user_register_time where pid in ('")
                .append(projects[0]).append("','").append(projects[1]).append("') group by orig_id");
        return sb.toString();
    }

    public static String getTransAttributeUIDSql(String project, String attribute, List<String> pids){
        StringBuffer sb = new StringBuffer();

        sb.append("insert overwrite table user_attribute  partition(pid='").append(project).append("',attr='").append(attribute).append("')")
                .append("select distinct lower(ui.orig_id), up.val from user_property up join user_id ui on up.uid = md5uid(ui.uid) and up.pid = ui.pid ")
                .append("where  up.prop = '").append(attribute).append("' and  up.pid in ('").append(pids.get(0)).append("'");
        for(int i=1;i<pids.size();i++){
            sb.append(",'").append(pids.get(i)).append("'");
        }
        sb.append(")");

        return sb.toString();
    }


}
