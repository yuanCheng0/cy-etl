package com.cy.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.Arrays;
import java.util.List;

/**
 * Created by cy on 2017/12/27 23:27.
 */
public class HBase {
    private HBase(){}
    private static HBaseService hBaseService;
    static {
        // TODO  根据配置文件来选择  HBase 官方 API 还是第三方API
        hBaseService = new HBaseServiceImpl();
        //hBaseService = new AsyncHBaseServiceImpl();
    }

    /**
     * 写入单条数据
     * @param tableName
     * @param put
     * @param isWaiting 是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     */
    public static void put(String tableName, Put put,boolean isWaiting){
        hBaseService.batchPut(tableName, Arrays.asList(put),isWaiting);
    }

    public static void put(String tableName, List<Put> puts,boolean isWaiting){
        hBaseService.put(tableName,puts,isWaiting);
    }

    /**
     * 获取多行数据
     * @param tableName
     * @param rows
     * @param <T>
     * @return
     */
    public static <T>Result[] getRows(String tableName,List<T> rows){
        return hBaseService.getRows(tableName,rows);
    }

    /**
     * 获取单条数据
     * @param tableName
     * @param row
     * @return
     */
    public static Result getRow(String tableName,String row){
        return hBaseService.getRow(tableName,row);
    }
}
