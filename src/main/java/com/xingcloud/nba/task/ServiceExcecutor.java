package com.xingcloud.nba.task;

import com.xingcloud.nba.service.BAService;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * 用于并发的执行service里的方法
 * Author: liqiang
 * Date: 14-8-27
 * Time: 下午2:18
 */
public class ServiceExcecutor implements Callable<Map<String, Map<String,Number[]>>> {

    private static final Logger LOGGER = Logger.getLogger(ServiceExcecutor.class);
    BAService service = new BAService();
    Task task;
    Set<String> projects;
    String attr;
    String day;

    public ServiceExcecutor(Task task,Set<String> projects, String day){
        this.task =  task;
        this.projects = projects;
        this.day = day;
    }

    public ServiceExcecutor(Task task,Set<String> projects, String attr, String day){
        this.task = task;
        this.projects = projects;
        this.attr = attr;
        this.day = day;
    }

    @Override
    public Map<String, Map<String,Number[]>> call() throws Exception {
        if(task == Task.ACTIVE){
            return service.calActiveUser(projects,day);
        }else if(task == Task.NEW){
            return service.calNewUser(projects,day);
        }else if(task == Task.RETAIN){
            return service.calRetainUser(projects,day);
        }else if(task == Task.COVER){
            return service.calNewCoverUser(projects,day);
        }else if(task == Task.ATTR_NEW){
            return service.calNewUserByAttr(projects,attr,day);
        }else if(task == Task.ATTR_RETAIN){
            return service.calRetentionUserByAttr(projects,attr,day);
        }
        return null;
    }
}