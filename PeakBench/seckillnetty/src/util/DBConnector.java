package util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class DBConnector {

	private String ip = null;
	private String port = null;
	private String dbname = null;
	private String userName = null;
	private String passwd = null;
	private String driver = null;
	private String db = null;
	
	public DBConnector(String ip, String port, String dbname, String userName,
			String passwd,String driver, String db) {
		super();
		this.ip = ip;
		this.port = port;
		this.dbname = dbname;
		this.userName = userName;
		this.passwd = passwd;
		this.driver = driver;
		this.db = db;
	}
	
	public DBConnector(String ip, String dbname, String userName,
			String passwd,String driver, String db) {
		super();
		this.ip = ip;
		this.dbname = dbname;
		this.userName = userName;
		this.passwd = passwd;
		this.driver = driver;
		this.db = db;
	}
	
	public Connection getConnection() throws FileNotFoundException {
		String url = "jdbc:"+db+"://" + ip + ":" + port + "/" + dbname;
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, userName, passwd);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	public Connection getNewConnection() throws FileNotFoundException {
		String url = "jdbc:"+db+"://" + ip + "/" + dbname;
		Connection conn = null;
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, userName, passwd);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
}
