package skread;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import util.DBConnector;

public class Synchronizer {

	private String updateLoadFile = null;
	private int concurrency;
	private int updateTimelife;
	private String ipFile = null;

	private ArrayList<DBConnector> dbConnector = null;

	public Synchronizer(String updateLoadFile, int concurrency, int updateTimelife,
			String ipFile) {
		super();
		this.updateLoadFile = updateLoadFile;
		this.concurrency = concurrency;
		this.updateTimelife = updateTimelife;
		this.ipFile = ipFile;
	}

	public void generate() {
		BufferedReader ipbr = null;
		try {
			ipbr = new BufferedReader(new InputStreamReader(new FileInputStream(ipFile)));
			String[] ip = null;
			String ipLine = null;
			while((ipLine = ipbr.readLine())!=null)
			{
				ip = ipLine.split(",");
				dbConnector.add(new DBConnector(ip[0], ip[1], ip[2], ip[3], ip[4],ip[5],ip[6]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(ipbr != null)	ipbr.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		CountDownLatch countDownLatch = new CountDownLatch(concurrency);
		File[] files = new File(updateLoadFile).listFiles();
		BufferedReader br = null;
		try {
			ArrayList<UpdateLoadThread> threads = new ArrayList<UpdateLoadThread>();
			for(int i = 0; i < concurrency; i++) {
				threads.add(new UpdateLoadThread(dbConnector.get(i%dbConnector.size())));
			}
			for(int i = 0; i < files.length; i++) {
				long start = System.currentTimeMillis();
				
				br = new BufferedReader(new InputStreamReader(new FileInputStream(files[i]), "utf-8"));
				String inputLine = null;
				ArrayList<String> sqlList1 = new ArrayList<String>();
				ArrayList<String> sqlList2 = new ArrayList<String>();
				while((inputLine = br.readLine()) != null) {
					if(inputLine.split("\t").length == 3) {
						sqlList1.add(inputLine);
					} else {
						sqlList2.add(inputLine);
					}
				}

				float skUpdateRatio = (float)sqlList1.size() / (sqlList1.size() + sqlList2.size());
				int skConcurrency = (int)(concurrency * skUpdateRatio);
				int nonskConcurrency = concurrency - skConcurrency;

				ArrayList<ArrayList<String>> sqlList1s = new ArrayList<ArrayList<String>>();
				for(int j = 0; j < skConcurrency; j++)
					sqlList1s.add(new ArrayList<String>());
				for(int j = 0; j < sqlList1.size(); j++) {
					sqlList1s.get(j % skConcurrency).add(sqlList1.get(j));
				}

				ArrayList<ArrayList<String>> sqlList2s = new ArrayList<ArrayList<String>>();
				for(int j = 0; j < nonskConcurrency; j++)
					sqlList2s.add(new ArrayList<String>());
				for(int j = 0; j < sqlList2.size(); j++) {
					sqlList2s.get(j % nonskConcurrency).add(sqlList2.get(j));
				}

				for(int j = 0; j < skConcurrency; j++) {
					threads.get(j).setInfo(true, countDownLatch, sqlList1s.get(j));
					new Thread(threads.get(j)).start();
				}

				for(int j = 0; j < nonskConcurrency; j++) {
					threads.get(skConcurrency + j).setInfo(false, countDownLatch, sqlList2s.get(j));
					new Thread(threads.get(skConcurrency + j)).start();
				}

				countDownLatch.await();
				long syncTime = System.currentTimeMillis() - start;
				System.out.println("sync time(slowest): " + syncTime + "ms");
				if(syncTime < updateTimelife)
					Thread.sleep(updateTimelife - syncTime);
				countDownLatch = new CountDownLatch(concurrency);
			}

			for(int i = 0; i < concurrency; i++) {
				threads.get(i).close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null)	br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	class UpdateLoadThread implements Runnable {

		private Connection conn = null;
		private PreparedStatement pstmt1 = null, pstmt2 = null;
		private Statement pst = null;
		private boolean flag;

		private CountDownLatch countDownLatch = null;
		private ArrayList<String> sqlList = null;

		public UpdateLoadThread(DBConnector dbConnector_i) {
			super();
			try{
				conn = dbConnector_i.getConnection();
			}catch (FileNotFoundException e1){
				e1.printStackTrace();
			}
			
			try {
				pst = conn.createStatement();
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt1 = conn.prepareStatement("update item set count = count + ?, paycount = paycount + ? where itemkey = ?");
				pstmt2 = conn.prepareStatement("update item set count = count + ? where itemkey = ?");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void run() {
			try {
				if(flag) {
					for(int i = 0; i < sqlList.size(); i++) {
						String[] para = sqlList.get(i).split("\t");
						pstmt1.setInt(1, Integer.parseInt(para[0]));
						pstmt1.setInt(2, Integer.parseInt(para[1]));
						pstmt1.setInt(3, Integer.parseInt(para[2]));
						pstmt1.addBatch();
					}
					pstmt1.executeBatch();
				} else {
					for(int i = 0; i < sqlList.size(); i++) {
						String[] para = sqlList.get(i).split("\t");
						pstmt2.setInt(1, Integer.parseInt(para[0]));
						pstmt2.setInt(2, Integer.parseInt(para[1]));
						pstmt2.addBatch();
					}
					pstmt2.executeBatch();
				}
				countDownLatch.countDown();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void setInfo(boolean flag, CountDownLatch countDownLatch, 
				ArrayList<String> sqlList) {
			this.flag = flag;
			this.countDownLatch = countDownLatch;
			this.sqlList = sqlList;
		}

		public void close() {
			try {
				if(pstmt1 != null)	pstmt1.close();
				if(pstmt2 != null)	pstmt2.close();
				if(conn != null)	conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException {
		String userDir = System.getProperty("user.dir");  
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/readDB.properties"));   
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}
		Synchronizer synchronizer = new Synchronizer(p.getProperty("updateLoadFile"),
				Integer.parseInt(p.getProperty("Synchronizer.concurrency")), Integer.parseInt(p.getProperty("updateTimelife")),
				p.getProperty("ipfile"));
		synchronizer.generate();
	}
}
