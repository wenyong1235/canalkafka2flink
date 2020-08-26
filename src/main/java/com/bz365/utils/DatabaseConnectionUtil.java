package com.bz365.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DatabaseConnectionUtil {

	public static final String DBDRIVER = ConfigUtil.mysqlDriverOut;
	public static final String DBURL = ConfigUtil.mysqlUrlOut;
	public static final String DBUSER = ConfigUtil.mysqlUserOut;
	public static final String DBOASSWORD = ConfigUtil.mysqlPSWOut;
	private Connection conn = null;
	
	public Connection getConnention() {
		try {
			Class.forName(DBDRIVER);
			conn = DriverManager.getConnection(DBURL, DBUSER, DBOASSWORD);
		}catch (Exception e) {
				 e.printStackTrace();
			 }
		 return conn;
		}


	public void close() {
		if(this.conn !=null) {
			try {
				this.conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
