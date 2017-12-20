package skwrite;
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

public class writedataimport
{
	private String skWriteFile = null;
	private String ip = null;
	private String port = null;
	private String dbname = null;
	private String userName = null;
	private String passwd = null;
	private String driver = null;
	private String db = null;
	private DBConnector dbConnector = null;

	public writedataimport(String skWriteFile, String ip, String port,
			String dbname, String userName, String passwd, String driver, String db) {
		super();
		this.skWriteFile = skWriteFile;
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
			new Thread(new DataimportThread(countDownLatch, skWriteFile, i)).start();
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
		private String skWriteFile = null;
		private int file_th;
		private Connection conn = null;
		private PreparedStatement pstmt = null;
		private Statement pst = null;

		public DataimportThread(CountDownLatch countDownLatch,
				String skWriteFile, int file_th) {
			super();
			this.countDownLatch = countDownLatch;
			this.skWriteFile = skWriteFile;
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
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skWriteFile + "//customer.csv")));
				conn = dbConnector.getConnection();
				pst = conn.createStatement();
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into customer (c_custkey, c_name, c_address) values (?,?,?)");
				while((sline = br.readLine()) != null)
				{
					if((line%200)==file_th)
					{
						skItemsInfo = sline.split(",");				
						pstmt.setInt(1,Integer.valueOf(skItemsInfo[0]));  
						pstmt.setString(2, skItemsInfo[1]);
						pstmt.setString(3, skItemsInfo[2]);
						pstmt.executeUpdate();            
					}
					line++;
				}	
				line = 0;
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skWriteFile + "//supplier.csv")));
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into supplier (s_suppkey, s_name, s_address) values (?,?,?)");
				while((sline = br.readLine()) != null)
				{
					if((line%200)==file_th)
					{
						skItemsInfo = sline.split(",");				
						pstmt.setInt(1,Integer.valueOf(skItemsInfo[0]));  
						pstmt.setString(2, skItemsInfo[1]);
						pstmt.setString(3, skItemsInfo[2]);
						pstmt.executeUpdate();            
					}
					line++;
				}
				line = 0;
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skWriteFile + "//item.csv")));
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into  item (i_itemkey, i_suppkey, i_name , i_count,"
						+ " i_price, i_detail, i_type, i_popularity) values (?,?,?,?,?,?,?,?)");
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
						pstmt.executeUpdate();            
					}
					line++;
				}
				line = 0;
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skWriteFile + "//orders.csv")));
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into orders (o_orderkey, o_custkey, o_skpkey, o_price,"
						+ "o_orderdate, o_paydate, o_state) values (?,?,?,?,?,?,?)");
				while((sline = br.readLine()) != null)
				{
					if((line%200)==file_th)
					{
						skItemsInfo = sline.split(",");				
						pstmt.setInt(1,Integer.valueOf(skItemsInfo[0])); 
						pstmt.setInt(2,Integer.valueOf(skItemsInfo[1])); 
						pstmt.setInt(3,Integer.valueOf(skItemsInfo[2]));
						pstmt.setDouble(4, Double.valueOf(skItemsInfo[3]));
						date = formatter.parse(skItemsInfo[4]);
						pstmt.setTimestamp(5,new Timestamp(date.getTime()));
						date = formatter.parse(skItemsInfo[5]);
						pstmt.setTimestamp(6,new Timestamp(date.getTime()));
						pstmt.setInt(7,Integer.valueOf(skItemsInfo[6])); 
						pstmt.executeUpdate();            
					}
					line++;
				}
				line = 0;
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skWriteFile + "//orderitem.csv")));
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into orderitem (oi_orderkey, oi_itemkey, oi_count, oi_price) values (?,?,?,?)");
				while((sline = br.readLine()) != null)
				{
					if((line%200)==file_th)
					{
						skItemsInfo = sline.split(",");				
						pstmt.setInt(1,Integer.valueOf(skItemsInfo[0]));  
						pstmt.setInt(2,Integer.valueOf(skItemsInfo[1]));
						pstmt.setInt(3,Integer.valueOf(skItemsInfo[2]));
						pstmt.setDouble(4, Double.valueOf(skItemsInfo[3]));
						pstmt.executeUpdate();            
					}
					line++;
				}
				line = 0;
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skWriteFile + "//seckillplan.csv")));
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into seckillplan (sl_skpkey, sl_itemkey, sl_price, "
						+ "sl_starttime, sl_endtime, sl_plancount, sl_skpcount, sl_popularity) values (?,?,?,?,?,?,?,?)");
				while((sline = br.readLine()) != null)
				{
					if((line%200)==file_th)
					{
						skItemsInfo = sline.split(",");				
						pstmt.setInt(1,Integer.valueOf(skItemsInfo[0]));  
						pstmt.setInt(2,Integer.valueOf(skItemsInfo[1]));
						pstmt.setDouble(3, Double.valueOf(skItemsInfo[2]));
						date = formatter.parse(skItemsInfo[3]);
						pstmt.setTimestamp(4,new Timestamp(date.getTime()));
						date = formatter.parse(skItemsInfo[4]);
						pstmt.setTimestamp(5,new Timestamp(date.getTime()));
						pstmt.setInt(6,Integer.valueOf(skItemsInfo[5]));  
						pstmt.setInt(7,Integer.valueOf(skItemsInfo[6]));
						pstmt.setInt(8,Integer.valueOf(skItemsInfo[7]));
						pstmt.executeUpdate();            
					}
					line++;
				}
				line = 0;
				br = new BufferedReader(new InputStreamReader(new FileInputStream(skWriteFile + "//seckillpay.csv")));
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt = conn.prepareStatement("insert into seckillpay (sa_skpkey, sa_paycount, sa_starttime, sa_endtime) values (?,?,?,?)");
				while((sline = br.readLine()) != null)
				{
					if((line%200)==file_th)
					{
						skItemsInfo = sline.split(",");				
						pstmt.setInt(1,Integer.valueOf(skItemsInfo[0]));  
						pstmt.setInt(2,Integer.valueOf(skItemsInfo[1]));
						date = formatter.parse(skItemsInfo[2]);
						pstmt.setTimestamp(3,new Timestamp(date.getTime()));
						date = formatter.parse(skItemsInfo[3]);
						pstmt.setTimestamp(4,new Timestamp(date.getTime()));
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
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/staticDataWrite.properties"));   
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}   
		writedataimport dataGenerator = new writedataimport( p.getProperty("skWriteFile"),p.getProperty("ip"), p.getProperty("port"),
				p.getProperty("dbname"), p.getProperty("userName"), p.getProperty("passwd"),
				p.getProperty("driver"), p.getProperty("db"));
		dataGenerator.generate();
		System.out.println("skwrite db gene time: " + ((System.currentTimeMillis() - time) / 1000) + "s");
	}
}