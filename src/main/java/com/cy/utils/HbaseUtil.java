package com.cy.utils;

import com.cy.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Hbase工具类
 * Created by cy on 2017/12/24 16:27.
 */
public class HbaseUtil {
    private static Logger log = LoggerFactory.getLogger(HbaseUtil.class);
    private static Connection conn;
    private static Configuration conf;
    static {
        PropertyUtil propertyUtil = PropertyUtil.getPropertyUtil("dev/conf.properties");
        if(conf == null){
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum",propertyUtil.getValue(Constants.QUORUM));
            conf.set("hbase.zookeeper.property.clientPort",propertyUtil.getValue(Constants.CLIENT_PORT));
        }
    }

    /**
     * 获取全局唯一点Configuration实例
     */
    /*public static synchronized Configuration getConf(){
        if(conf == null){
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum",QUORUM);
            conf.set("hbase.zookeeper.property.clientPort",CLIENT_PORT);
        }
        return conf;
    }*/

    /**
     * 获取全局唯一点Connection实例
     * @return
     */
    public static synchronized Connection getConn (){
        if(conn == null){
            try {
                conn = ConnectionFactory.createConnection(conf);
                return conn;
            } catch (IOException e) {
                log.error("获取连接错误：",e);
            } finally {
            }
        }
        return null;
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param cf 列族
     */
    public static void createOrOverwriteTable(String tableName,String cf) {
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            //列族压缩,默认为NODE。CompressionType 是一般的压缩，CompactionCompressionType是合并压缩。
            table.addFamily(new HColumnDescriptor(cf).setCompressionType(Compression.Algorithm.NONE));
            log.info("开始创建表：" + tableName);
            if(admin.tableExists(table.getTableName())){
                admin.disableTable(table.getTableName());//禁用
                admin.deleteTable(table.getTableName());//删除
                log.info(tableName + " 表已存在，禁用，并删除成功！");
            }
            admin.createTable(table);
            log.info("创建成功：" + tableName);
        } catch (IOException e) {
            log.error(tableName + "表创建失败：" + e);
            e.printStackTrace();
        }
    }

    /**
     * 增加列族
     * @param originalTableName 表名
     * @param familyName 列族名
     */
    public static void addFamilyName(String originalTableName,String familyName){
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            TableName tableName = TableName.valueOf(originalTableName);
            if (!admin.tableExists(tableName)){
                log.error(originalTableName + " Table does not exist.");
                return;
            }
            HTableDescriptor table = admin.getTableDescriptor(tableName);
            HColumnDescriptor newColumn = new HColumnDescriptor(familyName);
            newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            admin.addColumn(tableName,newColumn);
            log.info("成功添加列族：" + familyName);
        } catch (Exception e) {
            log.error("新增列族失败：" + e);
            e.printStackTrace();
        }
    }

    /**
     * 更新列族属性 ====》 待完善
     * @param originalTableName 表名
     * @param familyName 列族名
     */
    public static void updateFamilyName(String originalTableName,String familyName){
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            TableName tableName = TableName.valueOf(originalTableName);
            if (!admin.tableExists(tableName)){
                log.error(originalTableName + " Table does not exist.");
                return;
            }
            HTableDescriptor table = admin.getTableDescriptor(tableName);
            HColumnDescriptor existingColumn = new HColumnDescriptor(familyName);
            existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
            existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
            table.modifyFamily(existingColumn);
            admin.modifyTable(tableName,table);
            log.info("成功修改列族：" + familyName);
        } catch (IOException e) {
            log.error("更新失败：" + e);
            e.printStackTrace();
        }
    }

    /**
     * 删除列族
     * @param originalTableName 表名
     * @param familyName  列族名
     */
    public static void deleteFamilyName(String originalTableName,String familyName){
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            TableName tableName = TableName.valueOf(originalTableName);
            if (!admin.tableExists(tableName)){
                log.error(originalTableName + " Table does not exist.");
                return;
            }
            admin.disableTable(tableName);
            //删除列族
            admin.deleteColumn(tableName,familyName.getBytes("UTF-8"));
            log.info("成功删除列族：" + familyName);
        } catch (IOException e) {
            log.error("删除列族失败：" + e);
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     * @param originalTableName 表名
     */
    public static void deleteTable(String originalTableName){
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            TableName tableName = TableName.valueOf(originalTableName);
            if (!admin.tableExists(tableName)){
                log.error(originalTableName + " Table does not exist.");
                return;
            }
            admin.disableTable(tableName);//禁用表
            admin.deleteTable(tableName);
            log.info("成功删除表：" + originalTableName);
        } catch (IOException e) {
            log.error("删除表失败：" + e);
            e.printStackTrace();
        }
    }

    public static void close(){
        IOUtils.closeStream(conn);
    }


// ====================================================================================

    //通过表名获取hbase表
   /* public static HTable getHTableByTableName(String tableName) throws Exception{
        log.info("获取hbase表");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.72.120");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        HTable hTable = new HTable(configuration, tableName);
        return hTable;
    }*/
    //通过表名获取hbase结果
    /*public static void getResult(HTable table,String tableName) throws Exception{
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
    }*/

    /**
     * 建议
     *  tablename & column family  --> 常量，HBaseTableContent
     *  列 -- value
     *  Map<String,Object>
     * @param table
     * @throws Exception
    */
    /*private static void put(HTable table)throws Exception{
        Put put = new Put(Bytes.toBytes("1004"));
        put.add(Bytes.toBytes("basicInfo"),Bytes.toBytes("age"),Bytes.toBytes("15"));
        table.put(put);
        log.info("插入成功！");
        table.close();
    }*/

    /*private static void delete(HTable table)throws Exception{
        Delete delete = new Delete(Bytes.toBytes("1004"));
        delete.deleteColumns(Bytes.toBytes("basicInfo"),Bytes.toBytes("age"));
        table.delete(delete);
        log.info("删除成功");
        table.close();
    }*/

    /*public static void main(String[] args){
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

    }*/
}
