package com.cy.utils;

import com.cy.utils.mdel.JdbcModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by cy on 2018/1/2 21:06.
 */
public class JdbcUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcUtil.class);
    private Connection connection;
    public JdbcUtil(JdbcModel model){
        try {
            Class.forName(model.getDriverClassName());
            this.connection = DriverManager.getConnection(model.getUrl(),model.getUsername(),model.getPassword());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("创建connection失败：" + e.getMessage());
        } catch (SQLException e) {
            LOGGER.error("数据库连接获取失败：" + e.getMessage());
            throw new RuntimeException("创建connection失败：" + e.getMessage());
        }
    }
    public Connection getConnection(){
        return this.connection;
    }
    public Statement getStatement(){
        try {
            return this.connection.createStatement();
        } catch (SQLException e) {
            LOGGER.error("创建statement失败：" + e.getMessage());
            throw new RuntimeException("创建statement失败：" + e.getMessage());
        }
    }
    public PreparedStatement getPreparedStatement(String sql){
        try {
            return this.connection.prepareStatement(sql);
        } catch (SQLException e) {
            LOGGER.info("创建preparedStatement失败：" + e.getMessage());
            throw new RuntimeException("创建preparedStatement失败：" + e.getMessage());
        }
    }

    public void setParams(PreparedStatement ps,List<Object> params) {
        try {
            for (int i = 0;i < params.size();i++){
                ps.setObject(i + 1,params.get(i));
            }
        } catch (SQLException e) {
            LOGGER.error("设置查询参数失败：" + e.getMessage());
            throw new RuntimeException("设置查询参数失败：" + e.getMessage());
        }
    }

    public ResultSet executeQuery(String sql,List<Object> params) {
        try {
            PreparedStatement ps = getPreparedStatement(sql);
            if(params != null && params.size() > 0){
                setParams(ps,params);
            }
            return ps.executeQuery();
        } catch (SQLException e) {
            LOGGER.info("获取查询结果失败：" + e.getMessage());
            throw new RuntimeException("获取查询结果失败：" + e.getMessage());
        }
    }
    public ResultSetMetaData getResultSetMetaData(String sql,List<Object> params){
        ResultSet rs = executeQuery(sql, params);
        ResultSetMetaData rsd = null;
        try {
            rsd = rs.getMetaData();
        } catch (SQLException e) {
            LOGGER.info("获取查询结果失败：" + e.getMessage());
            throw new RuntimeException("获取查询结果失败：" + e.getMessage());
        }
        return rsd;
    }

    public List<Map<String,Object>> getResult(String sql,List<Object> params){
        List<Map<String,Object>> list = new ArrayList<>();
        Map<String,Object> map = null;
        ResultSet rs = executeQuery(sql, params);
        ResultSetMetaData rsd = getResultSetMetaData(sql, params);
        try {
            int columnCount = rsd.getColumnCount();
            while (rs.next()){
                map = new HashMap<>();
                for (int i = 0;i < columnCount;i++){
                    map.put(rsd.getColumnLabel(i + 1).toLowerCase(),rs.getObject(i + 1));
                }
                list.add(map);
            }
            return list;
        } catch (SQLException e) {
            LOGGER.error("获取查询结果集异常：" + e.getMessage());
            throw new RuntimeException("获取查询结果集异常：" + e.getMessage());
        }
    }
    public <T> List<T> getBeanResult(String sql,Class<T> clz,List<Object> params){
        List<T> list = new ArrayList<>();
        T t = null;
        Field[] fields = clz.getDeclaredFields();
        ResultSet rs = executeQuery(sql, params);
        ResultSetMetaData rsd = getResultSetMetaData(sql, params);
        try {
            int columnCount = rsd.getColumnCount();
            while (rs.next()){
                t = (T)clz.newInstance();
                for (int i = 0;i < fields.length;i++){
                    String columnLabel = rsd.getColumnLabel(i + 1);
                    Field field = fields[i];
                    if(!(field.getName().equalsIgnoreCase(columnLabel))){
                        continue;
                    }
                    boolean bFlag = field.isAccessible();
                    field.setAccessible(true);//打开javabean访问权限
                    field.set(t,rs.getObject(i + 1));
                    field.setAccessible(bFlag);
                }
            }
            list.add(t);
        } catch (SQLException e) {
            LOGGER.error("获取查询结果集异常：" + e.getMessage());
            throw new RuntimeException("获取查询结果集异常：" + e.getMessage());
        } catch (InstantiationException e) {
            LOGGER.error("获取查询结果集异常：" + e.getMessage());
            throw new RuntimeException("获取查询结果集异常：" + e.getMessage());
        } catch (IllegalAccessException e) {
            LOGGER.error("获取查询结果集异常：" + e.getMessage());
            throw new RuntimeException("获取查询结果集异常：" + e.getMessage());
        }
        return list;
    }
    public int executeJdbc(String sql,List<Object> params){
        int num = 0;
        PreparedStatement ps = getPreparedStatement(sql);
        setParams(ps,params);
        try {
            num = ps.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("执行sql失败：" + e.getMessage());
            throw new RuntimeException("执行sql失败：" + e.getMessage());
        }
        return num;
    }
    public void insertBatch(List<String> sqls,List<Object> params){

    }
}
