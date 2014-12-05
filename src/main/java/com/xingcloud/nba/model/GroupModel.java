package com.xingcloud.nba.model;

import com.google.gson.internal.Pair;

/**
 * Created by wanghaixing on 14-12-4.
 */
public class GroupModel {
    private Pair<String,CacheModel> nation;
    private Pair<String,CacheModel> ev3;
    private Pair<String,CacheModel> ev4;
    private Pair<String,CacheModel> ev5;

    public Pair<String, CacheModel> getNation() {
        return nation;
    }

    public void setNation(Pair<String, CacheModel> nation) {
        this.nation = nation;
    }

    public Pair<String, CacheModel> getEv3() {
        return ev3;
    }

    public void setEv3(Pair<String, CacheModel> ev3) {
        this.ev3 = ev3;
    }

    public Pair<String, CacheModel> getEv4() {
        return ev4;
    }

    public void setEv4(Pair<String, CacheModel> ev4) {
        this.ev4 = ev4;
    }

    public Pair<String, CacheModel> getEv5() {
        return ev5;
    }

    public void setEv5(Pair<String, CacheModel> ev5) {
        this.ev5 = ev5;
    }
}
