package com.cy.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public interface HBaseService {
    public void createTable(String tableName,String[] columnFamilies,boolean preBuildRegion) throws Exception;
    public void put(String tableName, Put put,boolean waiting);
    public void batchPut(String tableName, final List<Put> puts,boolean waiting);
    public <T> Result[] getRows(String tableName,List<T> rows);
    public Result getRow(String tableName,String row);
}
