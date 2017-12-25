package com.cy.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 成圆 on 2017/12/24 16:27.
 */
public class HbaseUtil {
    private static Logger log = LoggerFactory.getLogger(HbaseUtil.class);
    //通过表名获取hbase表
    public static HTable getHTableByTableName(String tableName) throws Exception{
        log.info("获取hbase表");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.72.120");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        HTable hTable = new HTable(configuration, tableName);
        return hTable;
    }
    //通过表名获取hbase结果
    public static void getResult(HTable table,String tableName) throws Exception{
        log.info("tableName" + tableName);
        Get get = new Get(Bytes.toBytes("Jack"));
        //================add column
        get.addColumn(Bytes.toBytes("basicInfo"),Bytes.toBytes("age"));

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        //key : rowkey + cf + c + version
        //value: value
        for (Cell cell : cells){
            log.info(
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    /**
     * 建议
     *  tablename & column family  --> 常量，HBaseTableContent
     *  列 -- value
     *  Map<String,Object>
     * @param table
     * @throws Exception
     */
    private static void put(HTable table)throws Exception{
        Put put = new Put(Bytes.toBytes("1004"));
        put.add(Bytes.toBytes("basicInfo"),Bytes.toBytes("age"),Bytes.toBytes("15"));
        table.put(put);
        log.info("插入成功！");
        table.close();
    }

    private static void delete(HTable table)throws Exception{
        Delete delete = new Delete(Bytes.toBytes("1004"));
        delete.deleteColumns(Bytes.toBytes("basicInfo"),Bytes.toBytes("age"));
        table.delete(delete);
        log.info("删除成功");
        table.close();
    }

    public static void main(String[] args){
        HTable table = null;
        ResultScanner scanner = null;
        try {
            table = getHTableByTableName("students");
            //getResult(students,"students");
            //put(table);
            //delete(table);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("Jack"));
            scan.setStopRow(Bytes.toBytes("Peter"));
            //scan.setCacheBlocks(""); 读取数据缓存到blockcache中去 boolean
            //scan.setCaching(""); 表示每次获取一条数据拿多少列的数据 int
            scanner = table.getScanner(scan);
            for (Result result : scanner){
                System.out.println(Bytes.toString(result.getRow()));
                System.out.println(result);
                System.out.println("------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(scanner);
            IOUtils.closeStream(table);
        }

    }
}
