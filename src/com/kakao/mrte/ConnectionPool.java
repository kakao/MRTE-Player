package com.kakao.mrte;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.cpdsadapter.DriverAdapterCPDS;
import org.apache.commons.dbcp.datasources.SharedPoolDataSource;

public class ConnectionPool {
	private final DataSource ds;

	public ConnectionPool(String host, int port, String user, String pass) {
		java.security.Security.setProperty("networkaddress.cache.ttl", "0");
		
		DriverAdapterCPDS cpds = new DriverAdapterCPDS();
		try {
			cpds.setDriver("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		cpds.setUrl("jdbc:mysql://"+host+":"+port+"/?autoReconnect=true");
		//cpds.setUrl("jdbc:mysql://mhatest.kakao.co.kr:3306/adm?autoReconnect=true");
		//cpds.setUrl("jdbc:mysql://mhatest.kakao.co.kr:3306/adm");
		cpds.setUser(user);
		cpds.setPassword(pass);

		SharedPoolDataSource tds = new SharedPoolDataSource();
		tds.setConnectionPoolDataSource(cpds);
		tds.setMaxActive(50);
		tds.setMaxWait(50);

		// tds.setTestWhileIdle(true);
		tds.setTestOnBorrow(true);
		tds.setValidationQuery("SELECT 1");
		// tds.setTimeBetweenEvictionRunsMillis(60000);
		tds.setTimeBetweenEvictionRunsMillis(6000000);

		this.ds = tds;
		
//		Connection[] conns = new Connection[15];
//		try{
//		for(int idx=0; idx<15; idx++){
//			conns[idx] = ds.getConnection();
//		}
//		for(int idx=0; idx<15; idx++){
//			if(conns[idx]!=null) conns[idx].close();
//		}
//		}catch(Exception ex){};
	}

	public Connection getConnection() throws SQLException {
		return this.ds.getConnection();
	}
}