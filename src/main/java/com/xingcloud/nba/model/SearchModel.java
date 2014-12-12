package com.xingcloud.nba.model;

/**
 * Created by wanghaixing on 14-12-12.
 */
public class SearchModel {
    private String nation;
    private String evt3;
    private String evt4;
    private String evt5;
    private CacheModel cacheModel;

    public String getNation() {
        return nation;
    }

    public void setNation(String nation) {
        this.nation = nation;
    }

    public String getEvt3() {
        return evt3;
    }

    public void setEvt3(String evt3) {
        this.evt3 = evt3;
    }

    public String getEvt4() {
        return evt4;
    }

    public void setEvt4(String evt4) {
        this.evt4 = evt4;
    }

    public String getEvt5() {
        return evt5;
    }

    public void setEvt5(String evt5) {
        this.evt5 = evt5;
    }

    public CacheModel getCacheModel() {
        return cacheModel;
    }

    public void setCacheModel(CacheModel cacheModel) {
        this.cacheModel = cacheModel;
    }
}
