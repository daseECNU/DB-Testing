package skwrite;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import util.DBConnector;

public class writetest
{
	public static float all_time = 0;
	private String ip = null;
	private String port = null;
	private String dbname = null;
	private String userName = null;
	private String passwd = null;
	private String driver = null;
	private String db = null;
	private int concurrency;
	private int itemnum;
	private int loadnum;
	private DBConnector dbConnector = null;
	public writetest( String ip, String port, String dbname,
			String userName, String passwd,String driver, String db, 
			int concurrency, int itemnum, int loadnum) {
		super();
		this.ip = ip;
		this.port = port;
		this.dbname = dbname;
		this.userName = userName;
		this.passwd = passwd;
		this.driver = driver;
		this.db = db;
		this.concurrency = concurrency;
		this.itemnum = itemnum;
		this.loadnum = loadnum;
	}
	public void generate() throws FileNotFoundException, SQLException
	{		
		dbConnector = new DBConnector(ip, port, dbname, userName, passwd,driver,db);
		Connection conn = null;
		Statement pst = null;
		conn = dbConnector.getConnection();
		pst = conn.createStatement();
	    pst.execute("delete from test2 where key2 >= 8000000;");
		System.out.println("start.........");
		long start = System.currentTimeMillis();
		CountDownLatch countDownLatch = new CountDownLatch(concurrency);
		int keyStart = 8000000;
		for(int i = 0; i < concurrency; i++) {
			new Thread(new testThread(countDownLatch, dbConnector, itemnum, loadnum/concurrency, keyStart)).start();
			keyStart += loadnum/concurrency;
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long allTime = (System.currentTimeMillis() - start);
		all_time += allTime;
		System.out.println("all time(slowest): " + allTime + "ms");
	}
	public static void main(String[] args) throws FileNotFoundException, SQLException {
		writetest writetest = new writetest("10.11.1.199", "5432", "writetest", "postgres", 
				"123456", "org.postgresql.Driver", "postgresql",25,1000000,1000000);
		int count =5;
		while(count-- > 0)
		{
			writetest.generate();
		}
		System.out.println("***********************");
		System.out.println("time: " + all_time/5 );
		System.out.println("***********************");
	}
}
class testThread implements Runnable {
	private CountDownLatch countDownLatch = null;
	private DBConnector dbConnector = null;
	private int itemnum;
	private int loadnum;
	private int keyStart;
	public testThread(CountDownLatch countDownLatch, DBConnector dbConnector,
			int itemnum, int loadnum, int keyStart){
		super();
		this.countDownLatch = countDownLatch;
		this.dbConnector = dbConnector;
		this.itemnum = itemnum;
		this.loadnum = loadnum;
		this.keyStart = keyStart;
	}
	public void run()
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = dbConnector.getConnection();
			pstmt = conn.prepareStatement("insert into test2 values (?, ?)");
			for(int i = 0; i < loadnum; i++)
			{
				int key = (int)(Math.random() * itemnum);
				pstmt.setInt(1, keyStart);
				pstmt.setInt(2, key);
				pstmt.executeUpdate();
				keyStart++;
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}finally {
			try {
				if(pstmt != null)	pstmt.close();
				if(conn != null)	conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("ok.........");
		countDownLatch.countDown();
	}
	
}
