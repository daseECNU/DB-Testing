package skwrite;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import util.DBConnector;

public class DataDelete
{
	private int ordernum = 0;
	private int workload = 0;
	private int sknum = 0;
	private String ip = null;
	private String port = null;
	private String dbname = null;
	private String userName = null;
	private String passwd = null;
	private String driver = null;
	private String db = null;
	private DBConnector dbConnector = null;

	public DataDelete(int ordernum, int workload, int sknum, String ip, String port,
			String dbname, String userName, String passwd, String driver, String db) {
		super();
		this.ordernum = ordernum;
		this.workload = workload;
		this.sknum = sknum;
		this.ip = ip;
		this.port = port;
		this.dbname = dbname;
		this.userName = userName;
		this.passwd = passwd;
		this.driver = driver;
		this.db = db;
	}
	public void generate(){
		dbConnector = new DBConnector(ip, port, dbname, userName, passwd,driver,db);
		long start = System.currentTimeMillis();
		int concurrency = 200;
		CountDownLatch countDownLatch = new CountDownLatch(concurrency);
		for(int i = 0; i < concurrency; i++) {
			new Thread(new DatadeleteThread(countDownLatch, ordernum, workload/concurrency)).start();
			ordernum += workload/concurrency;
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Connection conn = null;
		Statement pst = null;
		PreparedStatement pstmt3 = null, pstmt4 = null;
		try
		{
			conn = dbConnector.getConnection();
			pst = conn.createStatement();
			pst.execute("set @@session.ob_query_timeout=9000000000;");
			pstmt3 = conn.prepareStatement("update seckillplan set sl_skpcount = 0 where sl_skpkey = ?;");
			pstmt4 = conn.prepareStatement("update seckillpay set sa_paycount = 0 where sa_skpkey = ?;");
			for(int i= 0 ; i<sknum;i++)
			{			
					pstmt3.setInt(1,i);  
					pstmt3.executeUpdate();  
					pstmt4.setInt(1,i);  
					pstmt4.executeUpdate();
			}	
	        conn.close();
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (SQLException e){
			e.printStackTrace();
		}
		System.out.println("ɾ����ݵ�ʱ�䣺" + (System.currentTimeMillis() - start) + "ms");
	}
	class DatadeleteThread implements Runnable {	
		private CountDownLatch countDownLatch = null;
		private int ordernumstar = 0;
		private int workloadnum;
		private Connection conn = null;
		private PreparedStatement  pstmt2 = null;
		private Statement pstmt1 = null, pst = null;

		public DatadeleteThread(CountDownLatch countDownLatch,
				int ordernumstar, int workloadnum) {
			super();
			this.countDownLatch = countDownLatch;
			this.ordernumstar = ordernumstar;
			this.workloadnum = workloadnum;
		}

		public void run() { 
			try
			{
				conn = dbConnector.getConnection();
				pst = conn.createStatement();
				pst.execute("set @@session.ob_query_timeout=9000000000;");
			    pstmt1 = conn.createStatement();
				pstmt2 = conn.prepareStatement("delete from orders where o_orderkey = ?");		
				for(int i= 0 ; i<workloadnum;i++)
				{		
					pstmt1.executeUpdate("delete from orderitem where oi_orderkey = "+ordernumstar);
					pstmt2.setInt(1,ordernumstar);
					pstmt2.executeUpdate();  
					ordernumstar++;
				}	
				pstmt1.close();
				pstmt2.close();
		        conn.close();  
			}catch (FileNotFoundException e){
				e.printStackTrace();
			}catch (SQLException e){
				e.printStackTrace();
			}
			countDownLatch.countDown();
		}
	}
	public static void main(String[] args) throws FileNotFoundException{
//		String userDir = System.getProperty("user.dir");  
//		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/staticDataWrite.properties"));   
//		Properties p = new Properties();   
//		try {   
//			p.load(inputStream);   
//		} catch (IOException e1) {   
//		   e1.printStackTrace();   
//		}   
		DataDelete datadelete = new DataDelete(4000000,370000,1000,"10.11.1.191","13998","mysql?useServerPrepStmts=true",
				"admin","admin","com.mysql.jdbc.Driver","mysql");
		datadelete.generate();
	}
}
