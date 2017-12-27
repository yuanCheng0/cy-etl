package com.cy.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * HBase服务抽象类
 * Created by cy on 2017/12/27 0:43.
 */
public abstract class AbstractHBaseService implements HBaseService{
    @Override
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {

    }

    @Override
    public void put(String tableName,List<Put> puts, boolean waiting) {

    }

    @Override
    public void batchPut(String tableName, List<Put> puts, boolean waiting) {

    }

    @Override
    public <T> Result[] getRows(String tableName, List<T> rows) {
        return new Result[0];
    }

    @Override
    public Result getRow(String tableName, String row) {
        return null;
    }
}
