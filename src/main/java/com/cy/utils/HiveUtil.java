package com.cy.utils;

import com.cy.utils.mdel.HiveConnectionModel;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * Created by cy on 2018/1/2 22:42.
 */
public class HiveUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveUtil.class);
    private TTransport transport = null;
    private ConnectionPool connectionPool = null;
    private HiveConnectionModel hcModel;

    public HiveUtil(HiveConnectionModel hcModel){
        this.hcModel = hcModel;
        this.createHiveConnectionPool();
    }
    private void createHiveConnectionPool(){
        try {
            this.connectionPool = new ConnectionPool(this.hcModel.getDriverClassName(),this.hcModel.getUrl(),this.hcModel.getUsername(),this.hcModel.getPassword());
            this.transport = new TSocket(this.hcModel.getIp(),Integer.parseInt(this.hcModel.getPort()));
            this.transport.open();
        } catch (TTransportException e) {
            LOGGER.error(e.getMessage(),e);
            throw new RuntimeException(e.getMessage());
        }
    }
    private synchronized Connection obtainConnection(){
        Connection connection = null;
        connection = this.connectionPool.getConnection();
        return connection;
    }
}
