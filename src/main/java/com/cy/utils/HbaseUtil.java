package com.cy.utils;

import com.cy.constants.Constants;
import com.cy.hbase.bo.HBasePageModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        try {
            if(conf == null){
                conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum",propertyUtil.getValue(Constants.QUORUM));
                conf.set("hbase.zookeeper.property.clientPort",propertyUtil.getValue(Constants.CLIENT_PORT));
            }
        } catch (Exception e) {
            log.error("HBase 初始化配置失败！");
            throw new RuntimeException(e);
        }
    }
    /**
     * 获取全局唯一点Connection实例
     * @return
     */
    public static synchronized Connection getConn (){
        if(conn == null || conn.isClosed()){
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                log.error("HBase建立连接失败：",e);
            }
        }
        return conn;
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族
     * @param preBuildRegion 是否建立预分区，true --> 是，false --> 否
     */
    public static void createOrOverwriteTable(String tableName,boolean preBuildRegion,String... familyNames) {
        Admin admin = null;
        try {
            if (preBuildRegion){
                String[] s = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};
                int partition = 16;
                byte[][] splitKeys = new byte[partition - 1][];
                for (int i = 1; i < partition;i++){
                    splitKeys[i -1] = Bytes.toBytes(s[i - 1]);
                }
                createOrOverwriteTable(tableName,splitKeys,familyNames);
            }else {
                createOrOverwriteTable(tableName,familyNames);
            }
        } catch (Exception e) {
            log.error(tableName + "表创建失败：" + e);
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @param tableName
     * @param familyNames
     */
    public static void createOrOverwriteTable(String tableName,String... familyNames) {
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            if(admin.tableExists(table.getTableName())){
                admin.disableTable(table.getTableName());//禁用
                admin.deleteTable(table.getTableName());//删除
                log.info(tableName + " 表已存在，禁用，并删除成功！");
            }
            for (String familyName : familyNames){
                //列族压缩,默认为NODE。CompressionType 是一般的压缩，CompactionCompressionType是合并压缩。
                table.addFamily(new HColumnDescriptor(familyName).setCompressionType(Compression.Algorithm.NONE));
            }
            log.info("开始创建表：" + tableName);
            admin.createTable(table);
            log.info("Table创建成功：" + tableName);
        } catch (IOException e) {
            log.error(tableName + " 表创建失败！" + e);
        }finally {
            IOUtils.closeStream(admin);
            close();
        }
    }
    /**
     * 创建预分区表
     * @param tableName 表名
     * @param splitKeys 分区字段
     * @param familyNames 列族名
     */
    public static void createOrOverwriteTable(String tableName,byte[][] splitKeys,String... familyNames) {
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            if(admin.tableExists(table.getTableName())){
                admin.disableTable(table.getTableName());//禁用
                admin.deleteTable(table.getTableName());//删除
                log.info(tableName + " 表已存在，禁用，并删除成功！");
            }
            for (String familyName : familyNames){
                //列族压缩,默认为NODE。CompressionType 是一般的压缩，CompactionCompressionType是合并压缩。
                table.addFamily(new HColumnDescriptor(familyName).setCompressionType(Compression.Algorithm.NONE));
            }
            log.info("开始创建表：" + tableName);
            admin.createTable(table,splitKeys);
            log.info("Table创建成功：" + tableName);
        } catch (IOException e) {
            log.error(tableName + " 表创建失败！" + e);
        }finally {
            IOUtils.closeStream(admin);
            close();
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
                log.warn(originalTableName + " Table does not exist.");
                return;
            }
            admin.disableTable(tableName);//禁用表
            admin.deleteTable(tableName);
            log.info("成功删除表：" + originalTableName);
        } catch (IOException e) {
            log.error("删除表失败：" + e);
        }finally {
            IOUtils.closeStream(admin);
            close();
        }
    }

    /**
     * 给originalTableName创建快照表
     * 快照表名称自动生成： originalTableName + "_snapshot"
     * @param originalTableName
     */
    public static void createSnapshotTable(String originalTableName){
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            TableName tableName = TableName.valueOf(originalTableName);
            if (!admin.tableExists(tableName)){
                log.warn(originalTableName + " Table does not exist.");
                return;
            }
            String snapshotTableName = originalTableName + "_snapshot";
            if (!admin.tableExists(TableName.valueOf(snapshotTableName))){
                log.warn(snapshotTableName + " Table is exist.");
                return;
            }
            admin.snapshot(snapshotTableName,tableName);
            log.info("快照表创建成功：" + snapshotTableName);
        } catch (IOException e) {
            log.error(originalTableName + " 快照表创建失败！",e);
        }finally {
            IOUtils.closeStream(admin);
            close();
        }
    }
    /**
     * 增加列族
     * @param originalTableName 表名
     * @param familyNames 列族名
     */
    public static void addFamilyName(String originalTableName,String... familyNames){
        Admin admin = null;
        try {
            admin = getConn().getAdmin();
            TableName tableName = TableName.valueOf(originalTableName);
            if (!admin.tableExists(tableName)){
                log.error(originalTableName + " Table does not exist.");
                return;
            }
            for (String familyName : familyNames){
                HColumnDescriptor newColumn = new HColumnDescriptor(familyName);
                newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
                newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
                admin.addColumn(tableName,newColumn);
                log.info("成功添加列族：" + familyName);
            }
        } catch (Exception e) {
            log.error("新增列族失败：" + e);
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(admin);
            close();
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
        }finally {
            IOUtils.closeStream(admin);
            close();
        }
    }

    /**
     * 删除列族
     * @param originalTableName 表名
     * @param familyNames  列族名
     */
    public static void deleteFamilyName(String originalTableName,String... familyNames){
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
            for (String familyName : familyNames){
                admin.deleteColumn(tableName,familyName.getBytes("UTF-8"));
                log.info("成功删除列族：" + familyName);
            }
        } catch (IOException e) {
            log.error("删除列族失败：" + e);
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(admin);
            close();
        }
    }

    /**
     * 分页检索数据
     * @param originalTableName 表名
     * @param startRowKey 起始行键(如果为null，则从表中第一行开始检索)
     * @param endRowKey 结束行健(如果为null，则检索到最后一行)
     * @param filterList 检索条件过滤器集合(不包含分页过滤器，可以为null)
     * @param maxVersions 指定最大版本数(如果为最大整数值，则检索所有版本；如果为最小整数值，则检索最新版本；否则只检索指定的版本数)
     * @param pageModel 分页模型
     */
    public static HBasePageModel scanResutByPageFilter(String originalTableName, byte[] startRowKey, byte[] endRowKey,
                                                       FilterList filterList, int maxVersions, HBasePageModel pageModel){
        if(pageModel == null){
            pageModel = new HBasePageModel(10);
        }
        if(maxVersions <= 0){
            //默认只检索最新版本
            maxVersions = Integer.MIN_VALUE;
        }
        pageModel.initStartTime();
        pageModel.initEndTime();
        if(StringUtils.isBlank(originalTableName)){
            return pageModel;
        }
        Table table = null;
        ResultScanner scanner = null;
        try {
            table = getConn().getTable(TableName.valueOf(originalTableName));
            int tempPageSize = pageModel.getPageSize();
            boolean isEmptyStartRowKey = false;
            if(startRowKey == null){
                //则读取表点第一行记录
                Result firstResult = queryFirstRowResult(originalTableName,filterList);
                if(firstResult.isEmpty()){
                    return pageModel;
                }
                startRowKey = firstResult.getRow();
            }
            if (pageModel.getPageStartRowKey() == null){
                isEmptyStartRowKey = true;
                pageModel.setPageStartRowKey(startRowKey);
            }else {
                if (pageModel.getPageEndRowKey() != null){
                    pageModel.setPageStartRowKey(pageModel.getPageEndRowKey());
                }
                //从第二页开始，每次多取一条记录，因为第一条记录是要删除的
                tempPageSize += 1;
            }
            Scan scan = new Scan();
            scan.setStartRow(pageModel.getPageStartRowKey());
            if(endRowKey != null){
                scan.setStopRow(endRowKey);
            }
            PageFilter pageFilter = new PageFilter(pageModel.getPageSize() + 1);
            if (filterList != null){
                filterList.addFilter(pageFilter);
                scan.setFilter(filterList);
            }else {
                scan.setFilter(pageFilter);
            }
            if (maxVersions == Integer.MAX_VALUE){
                scan.setMaxVersions();
            }else if (maxVersions == Integer.MIN_VALUE){

            }else {
                scan.setMaxVersions(maxVersions);
            }
            scanner = table.getScanner(scan);
            List<Result> resultList = new ArrayList<>();
            int index = 0;
            for (Result rs : scanner.next(tempPageSize)){
                if(isEmptyStartRowKey == false && index == 0){
                    index += 1;
                    continue;
                }
                if (!rs.isEmpty()){
                    resultList.add(rs);
                }
                index += 1;
            }
            scanner.close();
            pageModel.setResultList(resultList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(scanner);
            IOUtils.closeStream(table);
            close();
        }
        int pageIndex = pageModel.getPageIndex() + 1;
        pageModel.setPageIndex(pageIndex);
        if(pageModel.getResultList().size() > 0){
            //获取本次分页数据首行和末行的行健信息
            byte[] pageStartRowKey = pageModel.getResultList().get(0).getRow();
            byte[] pageEndRowKey = pageModel.getResultList().get(pageModel.getResultList().size() - 1).getRow();
            pageModel.setPageStartRowKey(pageStartRowKey);
            pageModel.setPageEndRowKey(pageEndRowKey);
        }
        int queryTotalCount = pageModel.getQueryTotalCount() + pageModel.getResultList().size();
        pageModel.setQueryTotalCount(queryTotalCount);
        pageModel.initEndTime();
        pageModel.printTimeInfo();
        return pageModel;
    }

    public static Result queryFirstRowResult(String originalTableName,FilterList filterList){
        if(StringUtils.isBlank(originalTableName)){
            log.error("表名错误！");
            return null;
        }
        Table table = null;
        try {
            table = getConn().getTable(TableName.valueOf(originalTableName));
            Scan scan = new Scan();
            if(filterList != null){
                scan.setFilter(filterList);
            }
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            int index = 0;//TODO ?
            while (iterator.hasNext()){
                Result result = iterator.next();
                if(index == 0){
                    scanner.close();
                    return result;
                }
            }
        } catch (IOException e) {
            log.error("获取失败：" + e);
        }finally {
            IOUtils.closeStream(table);
        }
        return null;
    }

    /**
     * 获取Table
     * @param originalTableName 表名
     * @return
     */
    public static Table getTable(String originalTableName){
        try {
            return getConn().getTable(TableName.valueOf(originalTableName));
        } catch (IOException e) {
            log.error("获取表实例失败：" + e);
        }
        return null;
    }

    /**
     * 删除单条数据
     * @param originalTableName 表名
     * @param row rowKey
     */
    public static void delete(String originalTableName,String row) {
        Table table = getTable(originalTableName);
        if(table != null){
            Delete delete = new Delete(row.getBytes());
            try {
                table.delete(delete);
            } catch (IOException e) {
                log.error("删除失败：" + e);
            }finally {
                IOUtils.closeStream(table);
            }
        }
    }

    /**
     * 删除多行数据
     * @param originalTableName
     * @param rows
     */
    public static void delete(String originalTableName,String[] rows){
        Table table = getTable(originalTableName);
        if (table != null){
            try {
                List<Delete> list = new ArrayList<>();
                for (String row : rows){
                    Delete d = new Delete(Bytes.toBytes(row));
                    list.add(d);
                }
                if (list.size() > 0){
                    table.delete(list);
                }
            } catch (IOException e) {
                log.error("删除失败：" + e);
            } finally {
                IOUtils.closeStream(table);
            }
        }
    }

    /**
     * 通过row获取单条数据
     * @param originalTableName
     * @param row
     * @return
     */
    public static Result getRow(String originalTableName,String row){
        Table table = getTable(originalTableName);
        Result rs = null;
        if (table != null){
            try {
                Get get = new Get(Bytes.toBytes(row));
                rs = table.get(get);
            } catch (IOException e) {
                log.error("获取数据失败：" + e);
            } finally {
                IOUtils.closeStream(table);
            }
        }
        return rs;
    }

    /**
     * 获取多行数据
     * @param originalTableName
     * @param rows
     * @param <T>
     * @return
     */
    public static <T> Result[] getRows(String originalTableName,List<T> rows){
        Table table = getTable(originalTableName);
        List<Get> gets = null;
        Result[] results = null;
        if (table != null){
            try {
                gets = new ArrayList<>();
                for (T row : rows) {
                    if(row != null){
                        gets.add(new Get(Bytes.toBytes(String.valueOf(row))));
                    }/*else {
                        throw new RuntimeException("hbase have no data.");
                    }*/
                }
                if (gets.size() > 0){
                    results = table.get(gets);
                }
            } catch (IOException e) {
               log.error("获取数据失败：" + e);
            } finally {
                IOUtils.closeStream(table);
            }
        }
        return results;
    }

    /**
     * 获取整张表
     * @param originalTableName
     * @return
     */
    public static ResultScanner getAll(String originalTableName){
        Table table = getTable(originalTableName);
        ResultScanner results = null;
        if (table != null){
            try {
                Scan scan = new Scan();
                scan.setCaching(1000);
                results = table.getScanner(scan);
            } catch (IOException e) {
               log.error("获取失败：" + e);
            } finally {
                IOUtils.closeStream(table);
            }
        }
        return results;
    }
    /**
     * 关闭连接
     */
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
