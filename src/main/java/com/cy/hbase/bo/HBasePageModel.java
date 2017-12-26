package com.cy.hbase.bo;

import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase表数据分页模型类
 * Created by cy on 2017/12/26 21:45.
 */
public class HBasePageModel implements Serializable{
    private static final Logger log = LoggerFactory.getLogger(HBasePageModel.class);
    private int pageSize = 100; //分页记录数量
    private int pageIndex = 0; //当前页序号
    private int prePageIndex = 1; //上一页序号
    private int nextPageIndex = 1; //下一页序号
    private int pageCount = 0; //分页总数
    private int pageFirstRowIndex = 1; //每页第一行序号
    private byte[] pageStartRowKey = null; //每页起始行健
    private byte[] pageEndRowKey = null; //每页结束行健
    private int queryTotalCount = 0; //检索总记录数
    private boolean hasNextPage = true; //是否有下一页
    private long startTime = System.currentTimeMillis();//初始化起始时间
    private long endTime = System.currentTimeMillis(); //初始化结束时间
    private List<Result> resultList = new ArrayList<>();//获取HBase检索结果集合

    public HBasePageModel(int pageSize){
        this.pageSize = pageSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public int getPrePageIndex() {
        return prePageIndex;
    }

    public void setPrePageIndex(int prePageIndex) {
        this.prePageIndex = prePageIndex;
    }

    public int getNextPageIndex() {
        return nextPageIndex;
    }

    public void setNextPageIndex(int nextPageIndex) {
        this.nextPageIndex = nextPageIndex;
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    public int getPageFirstRowIndex() {
        this.pageFirstRowIndex = (this.getPageIndex() -1) * this.getPageSize() + 1;
        return pageFirstRowIndex;
    }

    public byte[] getPageStartRowKey() {
        return pageStartRowKey;
    }

    public void setPageStartRowKey(byte[] pageStartRowKey) {
        this.pageStartRowKey = pageStartRowKey;
    }

    public byte[] getPageEndRowKey() {
        return pageEndRowKey;
    }

    public void setPageEndRowKey(byte[] pageEndRowKey) {
        this.pageEndRowKey = pageEndRowKey;
    }

    public int getQueryTotalCount() {
        return queryTotalCount;
    }

    public void setQueryTotalCount(int queryTotalCount) {
        this.queryTotalCount = queryTotalCount;
    }

    public boolean isHasNextPage() {
        //TODO 这个判断是不严谨的，因为很有可能剩余的数据刚好够一页
        if(this.getResultList().size() == this.getPageSize()){
            this.hasNextPage = true;
        }else {
            this.hasNextPage = false;
        }
        return hasNextPage;
    }

    public void initStartTime() {
        this.startTime = System.currentTimeMillis();
    }

    public void initEndTime() {
        this.endTime = System.currentTimeMillis();
    }

    public List<Result> getResultList() {
        return resultList;
    }

    public void setResultList(List<Result> resultList) {
        this.resultList = resultList;
    }

    /**
     * 获取毫秒格式的耗时信息
     * @return
     */
    public String getTimeIntervalByMilli() {
        return String.valueOf(this.endTime - this.startTime) + "毫秒";
    }

    /**
     * 获取秒格式的耗时信息
     * @return
     */
    public String getTimeIntervalBySecond() {
        double interval = (this.endTime - this.startTime)/1000.0;
        DecimalFormat df = new DecimalFormat("#.##");
        return df.format(interval) + "秒";
    }

    /**
     * 打印时间信息
     */
    public void printTimeInfo() {
        log.info("起始时间：" + this.startTime);
        log.info("截止时间：" + this.endTime);
        log.info("耗费时间：" + this.getTimeIntervalBySecond());
    }
}
