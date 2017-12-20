package skread;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import util.DBConnector;

public class DataImport
{
	private String skReadFile = null;
	private String ip = null;
	private String port = null;
	private String dbname = null;
	private String userName = null;
	private String passwd = null;
	private String driver = null;
	private String db = null;
	private DBConnector dbConnector = null;

	public DataImport(String skReadFile, String ip, String port,
			String dbname, String userName, String passwd, String driver, String db) {
		super();
		this.skReadFile = skReadFile;
		this.ip = ip;
		this.port = port;
		this.dbname = dbname;
		this.userName = userName;
		this.passwd = passwd;
		this.driver = driver;
		this.db = db;
	}
	public void generate() {
		dbConnector = new DBConnector(ip, port, dbname, userName, passwd,driver,db);
		long start = System.currentTimeMillis();
		CountDownLatch countDownLatch = new CountDownLatch(200);
		for(int i = 0; i <  200; i++) {
			new Thread(new DataimportThread(countDownLatch, skReadFile, i)).start();
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("������ݵ�ʱ�䣺" + (System.currentTimeMillis() - start) + "ms");
	}
	class DataimportThread implements Runnable {	
		private CountDownLatch countDownLatch = null;
		private String skReadFile = null;
		private int file_th;
		private Connection conn = null;
		private PreparedStatement pstmt = null;
		private Statement pst = null;

		public DataimportThread(CountDownLatch countDownLatch,
				String skReadFile, int file_th) {
			super();
			this.countDownLatch = countDownLatch;
			this.skReadFile = skReadFile;
			this.file_th = file_th;
		}

		public void run() {
			int line = 0;
			BufferedReader br = null;
			String sline = null;
			String[] skItemsInfo = new String[12];
			java.util.Date date = null;
			SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
			try
			{
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skReadFile)));
				conn = dbConnector.getConnection();
				pst = conn.createStatement();
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into item (itemkey, suppkey, name, count, price,"
						+ " detail, type, popularity, skpkey, paycount, starttime, endtime) values (?,?,?,?,?,?,?,?,?,?,?,?)");
				while((sline = br.readLine()) != null)
				{
					if((line%200)==file_th)
					{
						skItemsInfo = sline.split(",");				
						pstmt.setInt(1,Integer.valueOf(skItemsInfo[0]));  
						pstmt.setInt(2,Integer.valueOf(skItemsInfo[1]));  
						pstmt.setString(3, skItemsInfo[2]);
						pstmt.setInt(4,Integer.valueOf(skItemsInfo[3]));
						pstmt.setDouble(5, Double.valueOf(skItemsInfo[4]));
						pstmt.setString(6, skItemsInfo[5]);
						pstmt.setInt(7,Integer.valueOf(skItemsInfo[6]));  
						pstmt.setInt(8,Integer.valueOf(skItemsInfo[7])); 
						pstmt.setInt(9,Integer.valueOf(skItemsInfo[8]));  
						pstmt.setInt(10,Integer.valueOf(skItemsInfo[9]));
						date = formatter.parse(skItemsInfo[10]);
						pstmt.setTimestamp(11,new Timestamp(date.getTime()));
						date = formatter.parse(skItemsInfo[11]);
						pstmt.setTimestamp(12,new Timestamp(date.getTime()));
						pstmt.executeUpdate();            
					}
					line++;
				}	
		        conn.close();  
			}catch (FileNotFoundException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}catch (SQLException e){
				e.printStackTrace();
			}catch (ParseException e){
				e.printStackTrace();
			}
			countDownLatch.countDown();
		}
	}
	public static void main(String[] args) throws FileNotFoundException{
		long time = System.currentTimeMillis();
		String userDir = System.getProperty("user.dir");  
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/staticDataRead.properties"));   
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}   
		DataImport dataGenerator = new DataImport( p.getProperty("skReadFile"),p.getProperty("ip"), p.getProperty("port"),
				p.getProperty("dbname"), p.getProperty("userName"), p.getProperty("passwd"),
				p.getProperty("driver"), p.getProperty("db"));
		dataGenerator.generate();
		System.out.println("skread db gene time: " + ((System.currentTimeMillis() - time) / 1000) + "s");
	}
}
