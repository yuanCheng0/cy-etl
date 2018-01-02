package com.cy.utils.mdel;

/**
 * Created by cy on 2018/1/2 21:03.
 */
public class HiveConnectionModel extends JdbcModel{
    private String ip;
    private String port;
    public HiveConnectionModel(String driverClassName, String url, String username, String password, String ip, String port) {
        super(driverClassName, url, username, password);
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}

