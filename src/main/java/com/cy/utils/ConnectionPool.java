package com.cy.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Vector;

/**
 * Created by cy on 2018/1/2 22:44.
 */
public class ConnectionPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtil.class);
    private String jdbcDriver = "";
    private String dbUrl = "";
    private String dbUsername = "";
    private String dbPassword = "";
    private String testTable = "";
    private int initialConnections = 1;
    private int incrementalConnections = 5;
    private int maxConnections = 50;

    public ConnectionPool(String jdbcDriver, String dbUrl, String dbUsername, String dbPassword) {
        this.jdbcDriver = jdbcDriver;
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
    }
    public Connection getConnection(){
        //TODO
        Connection connection = null;
        return connection;
    }
}
