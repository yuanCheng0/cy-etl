package com.cy.hbase;

import com.cy.utils.HbaseUtil;
import com.cy.utils.ThreadPoolUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by cy on 2017/12/27 23:29.
 */
public class HBaseServiceImpl extends AbstractHBaseService{
    private static final Logger log = LoggerFactory.getLogger(HBaseServiceImpl.class);
    /**
     * 获取线程池实例
     */
    private ThreadPoolUtil threadPool = ThreadPoolUtil.getThreadPoolUtil();

    /**
     * 多线程同步提交
     * @param tableName 表名
     * @param puts 待提交参数
     * @param isWaiting 是否等待线程执行完成，true可以及时看到结果，false让线程继续执行，并跳出此方法返回调用主程序
     */
   /* @Override
    public void put(final String tableName, final List<Put> puts, boolean isWaiting) {
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                HbaseUtil.put(tableName,puts);
            }
        });
        if(isWaiting){
            try {
                threadPool.awaitTermination();
            } catch (InterruptedException e) {
                log.error("HBase put job thread pool await termination time out." + e);
            }
        }
    }*/

    @Override
    public <T> Result[] getRows(String tableName, List<T> rows) {
        return HbaseUtil.getRows(tableName,rows);
}

    @Override
    public Result getRow(String tableName, String row) {
        return HbaseUtil.getRow(tableName,row);
    }

    /**
     * 多线程异步提交数据
     * @param tableName
     * @param puts
     * @param isWaiting
     */
    @Override
    public void put(final String tableName,final List<Put> puts,boolean isWaiting){
        Future f = threadPool.submit(new Runnable() {
            @Override
            public void run() {
                HbaseUtil.put(tableName,puts);
            }
        });
        if(isWaiting){
            try {
                f.get();
            } catch (InterruptedException e) {
                log.error("多线程异步提交返回数据执行失败.", e);
            } catch (ExecutionException e) {
                log.error("多线程异步提交返回数据执行失败.", e);
            }
        }
    }

    /**
     * 创建表
     * @param tableName         表名称
     * @param columnFamilies   列族名称数组
     * @param preBuildRegion  是否预分配Region   true 是  ， false 否  默认 16个region，rowkey生成的时候记得指定前缀
     */
    @Override
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
        HbaseUtil.createOrOverwriteTable(tableName,preBuildRegion,columnFamilies);
    }
}
