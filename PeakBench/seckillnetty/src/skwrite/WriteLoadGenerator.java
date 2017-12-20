package skwrite;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import util.DBConnector;
import util.RanDataGene;

public class WriteLoadGenerator {

	private String orderTransFile = null;
	private String ipFile = null;
	private int ipcount = 0;
	private String tpcfile = null;
	private int orderkeyStart;
	private int concurrency;

	private ArrayList<DBConnector> dbConnector = null;
	private ArrayList<ArrayList<String>> orderTransList = null;
	//传递custkey
	private Vector<ArrayList<Integer>> custkeyLists = null;
	private ActiveSubmitThreadNum activeThreads = null;

	private int orderHandleDelayTime;
	private int orderHandleConcurrency;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	private int limitSize;
	
	private int queueLengthControlDelayTime;
	private int cycle;
	private int gathertime;
	
	public static float all_time = 0,submit_time = 0; 
	public static float submittime = 0,paytime = 0,canceltime = 0;
	public static int all_tps = 0, all_count = 0, submit_tps = 0, submit_count = 0;

	public WriteLoadGenerator(String orderTransFile, int orderkeyStart,
			int concurrency, String ipFile,int ipcount,int orderHandleDelayTime,
			int orderHandleConcurrency, float payRatio,float cancelRatio, 
			float queryRatio,int limitSize, int queueLengthControlDelayTime, 
			int cycle, String tpcfile, int gathertime) {
		super();
		this.orderTransFile = orderTransFile;
		this.orderkeyStart = orderkeyStart;
		this.concurrency = concurrency;
		this.ipFile = ipFile;
		this.ipcount = ipcount;
		this.orderHandleDelayTime = orderHandleDelayTime;
		this.orderHandleConcurrency = orderHandleConcurrency;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		this.limitSize = limitSize;
		this.queueLengthControlDelayTime = queueLengthControlDelayTime;
		this.cycle = cycle;
		this.tpcfile = tpcfile;
		this.gathertime = gathertime;
		init();
	}

	private void init() {
		BufferedReader ipbr = null;
		try {
			ipbr = new BufferedReader(new InputStreamReader(new FileInputStream(ipFile)));
			String[] ip = null;
			String ipLine = null;
			dbConnector = new ArrayList<DBConnector>();
			while((ipLine = ipbr.readLine())!=null)
			{
				ip = ipLine.split(",");
				
				dbConnector.add(new DBConnector(ip[0], ip[1], ip[2], ip[3], ip[4], ip[5], ip[6]));
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
		

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(orderTransFile)));
			String inputLine = null;
			ArrayList<String> orderTrans = new ArrayList<String>();
			while((inputLine = br.readLine()) != null) {
				orderTrans.add(inputLine);
			}

			orderTransList = new ArrayList<ArrayList<String>>();
			for(int i = 0; i < concurrency; i++)
				orderTransList.add(new ArrayList<String>());

			for(int i = 0; i < orderTrans.size(); i++) {
				orderTransList.get(i % concurrency).add(orderTrans.get(i));
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

	@SuppressWarnings("resource")
	public void generate() throws FileNotFoundException, SQLException {
		Connection conn = null;
		Statement pst = null;
		conn = dbConnector.get(0).getConnection();
		pst = conn.createStatement();
		pst.execute("set @@session.ob_query_timeout=9000000000;");
//	    pst.execute("delete from orderitem where oi_orderkey >= 4000000;");
//	    pst.execute("delete from orders where o_orderkey >= 4000000;");
//	    pst.execute("update seckillplan set sl_skpcount = 0;");
//	    pst.execute("update seckillpay set sa_paycount = 0;");
	    custkeyLists = new Vector<ArrayList<Integer>>();
		activeThreads = new ActiveSubmitThreadNum(concurrency);
		long start = System.currentTimeMillis();
		final CountDownLatch countDownLatch1 = new CountDownLatch(concurrency);
		final CopyOnWriteArrayList<long[]> responseTimeList1 = new CopyOnWriteArrayList<long[]>();
		for(int i = 0; i < concurrency; i++) {
			responseTimeList1.add(new long[0]);
		}
				
		final CountDownLatch countDownLatch2 = new CountDownLatch(orderHandleConcurrency);
		final CopyOnWriteArrayList<long[]> responseTimeList2 = new CopyOnWriteArrayList<long[]>();
		for(int i = 0; i < orderHandleConcurrency; i++) {
			responseTimeList2.add(new long[0]);
		}

//		System.out.println("info of all submit threads[time1, time2, time3, time4, time5, transtime, count, failedcount, spend_time]:");

		for(int i = 0; i < concurrency; i++) {
			new Thread(new OrderSubmitThread(orderTransList.get(i), dbConnector.get(i%ipcount), orderkeyStart, 
					countDownLatch1, responseTimeList1, custkeyLists, activeThreads, i)).start();
			orderkeyStart += orderTransList.get(i).size();
		}

		for(int i = 0; i < orderHandleConcurrency; i++) {
			new Thread(new OrderHandleThread(dbConnector.get(i%ipcount), orderHandleDelayTime, payRatio, cancelRatio, queryRatio, custkeyLists, 
					activeThreads, limitSize, countDownLatch2, responseTimeList2, i)).start();
		}
		
		boolean[] flag = {false};
		new Thread(new QueueLengthControlThread(dbConnector.get(ipcount-1), queueLengthControlDelayTime, cycle, flag)).start();
		
		final CountDownLatch countDownLatch3 = new CountDownLatch(1);
		new Timer().schedule(new TimerTask() {  
			 @SuppressWarnings("unused")
			long time = 0, count = 0;
			int allCount = 0, failedCount = 0;
     		float tps_count = 0;
            public void run() {  
                // TODO Auto-generated method stub  
//                System.out.println("bombing!");  
            	time = 0;allCount = 0; failedCount = 0; tps_count = 0;
        		for(int j = 0; j < concurrency; j++) {
        			time += responseTimeList1.get(j)[5];
        			allCount += responseTimeList1.get(j)[6];
        			failedCount += responseTimeList1.get(j)[7];
        		}
        		System.out.println("submit[ " + allCount + "] avg response time: " + 
        				((float)time /  allCount)+ "us");
        		System.out.println("submit TPS: " + ((allCount - failedCount) * 1000 / gathertime ));
        		System.out.println("success: " + (allCount - failedCount));
        		System.out.println("failed: " + failedCount);
        		System.out.println("all submit: " + allCount);
        		time = 0; count = 0;
        		for(int j = 0; j < orderHandleConcurrency; j++) {
        			time += responseTimeList2.get(j)[24];
        			count += responseTimeList2.get(j)[25];
        		}
        		System.out.println("pay avg response time: " + ((float)time / count) + "us");
        		long payCountRec = count;
        		
        		time = 0; count = 0;
        		for(int j = 0; j < orderHandleConcurrency; j++) {
        			time += responseTimeList2.get(j)[26];
        			count += responseTimeList2.get(j)[27];
        		}
        		System.out.println("cancel avg response time: " + ((float)time / count) + "us");
        		System.out.println("pay & cancle: " + (payCountRec + count));
        		System.out.println("sucess submit & pay & cancle: " + (payCountRec + count + allCount- failedCount));
        		System.out.println("TPS: " + ((payCountRec + count + allCount - failedCount) * 1000 / gathertime));
        		
        		if(countDownLatch1.getCount() == 0 && countDownLatch2.getCount() == 0)
                {
                	cancel();
                	countDownLatch3.countDown();
                }
            }  
        }, 5000, gathertime);
		
		try {
			countDownLatch1.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		long submitTime = (System.currentTimeMillis() - start);
		submit_time += submitTime;
		
//		System.out.println("info of all handle threads[time1, count1, time2, count2, time3, count3, time4, count4, time5, count5, " +
//				"time6, count6, time7, count7, time8, count8, time9, count9, time10, count10, time11, count11, time12, count12, " +
//		"transTime1, transCount1, transTime2, transCount2, filedTrans]:");

		try {
			countDownLatch2.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		flag[0] = true;

		long allTime = (System.currentTimeMillis() - start);
		all_time += allTime;
		System.out.println("all time(slowest): " + allTime + "ms");

		try {
			countDownLatch3.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		int allCount = 0, failedCount = 0;
		long time=0;
		float tps_count = 0;
		for(int j = 0; j < concurrency; j++) {
			time += responseTimeList1.get(j)[5];
			allCount += responseTimeList1.get(j)[6];
			failedCount += responseTimeList1.get(j)[7];
			tps_count += ( (float)(responseTimeList1.get(j)[6]-responseTimeList1.get(j)[7])*1000)/ responseTimeList1.get(j)[8];
		}
		submittime += (float)time /  allCount; 
		System.out.println("submit[ " + allCount + "] avg response time: " + 
				((float)time /  allCount)+ "us");
		submit_tps += (allCount - failedCount) * 1000 / submitTime ;
		submit_count += allCount - failedCount;
		System.out.println("submit TPS: " + ((allCount - failedCount) * 1000 / submitTime ));
		System.out.println("submit TPS_count: " + tps_count);
		System.out.println("submit time: " + submitTime);
		System.out.println("success: " + (allCount - failedCount));
		System.out.println("failed: " + failedCount);
		System.out.println("all submit: " + allCount);
		
		for(int i = 0; i < 5; i++) {
			time = 0;
			for(int j = 0; j < concurrency; j++) {
				time += responseTimeList1.get(j)[i];
			}
			System.out.println("operation " + (i + 1) + " avg response time: " + 
					((float)time / (i <= 1 ? allCount : allCount - failedCount)) + "us");
		}		
		
		long count = 0;
		for(int i = 0; i < 24; i+=2) {
			time = 0; count = 0;
			for(int j = 0; j < orderHandleConcurrency; j++) {
				time += responseTimeList2.get(j)[i];
				count += responseTimeList2.get(j)[i + 1];
			}
//			System.out.println("operation " + (i / 2 + 1) + "[" + count + "] avg response time: " + ((float)time / count) + "us");
		}

		time = 0; count = 0;
		for(int j = 0; j < orderHandleConcurrency; j++) {
			time += responseTimeList2.get(j)[24];
			count += responseTimeList2.get(j)[25];
		}
		paytime += (float)time / count;
//		System.out.println("pay[" + count + "] avg response time: " + ((float)time / count) + "us");

		long payCountRec = count;
		time = 0; count = 0;
		for(int j = 0; j < orderHandleConcurrency; j++) {
			time += responseTimeList2.get(j)[26];
			count += responseTimeList2.get(j)[27];
		}
		canceltime += (float)time / count;
//		System.out.println("cancle[" + count + "] avg response time: " + ((float)time / count) + "us");

		System.out.println("pay & cancle: " + (payCountRec + count));
		System.out.println("sucess submit & pay & cancle: " + (payCountRec + count + allCount- failedCount));
		all_count += payCountRec + count + allCount- failedCount;
		all_tps += (payCountRec + count + allCount - failedCount) * 1000 / allTime;
		System.out.println("TPS: " + ((payCountRec + count + allCount - failedCount) * 1000 / allTime));
		
		BufferedWriter bw = null;
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tpcfile, true)));
		try
		{
			bw.write((payCountRec + count + allCount - failedCount) * 1000 / allTime +  "\n");
			bw.flush();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		count = 0;
		for(int j = 0; j < orderHandleConcurrency; j++) {
			count += responseTimeList2.get(j)[28];
		}
		System.out.println("rollback: " + count);
	}

	public static void main(String[] args) throws FileNotFoundException, SQLException {
		String userDir = System.getProperty("user.dir");  
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/writeDB.properties"));  
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}
		int count = 100,i=0;
		while(i++ < count)
		{
			WriteLoadGenerator writeLoadGenerator = new WriteLoadGenerator(p.getProperty("ordertransfile"), 
					(Integer.parseInt(p.getProperty("orders"))*Integer.parseInt(p.getProperty("scalFactor"))),
					Integer.parseInt(p.getProperty("submitconcurrency")), 
					p.getProperty("ipfile"), Integer.parseInt(p.getProperty("ipcount")),
					Integer.parseInt(p.getProperty("orderHandleDelayTime")), 
					Integer.parseInt(p.getProperty("orderHandleConcurrency")),
					Float.parseFloat(p.getProperty("payRatio")),Float.parseFloat(p.getProperty("cancelRatio")),Float.parseFloat(p.getProperty("queryRatio")), 
					Integer.parseInt(p.getProperty("limitSize")), Integer.parseInt(p.getProperty("queueLengthControlDelayTime")), 
					Integer.parseInt(p.getProperty("cycle")),p.getProperty("tpcfile"), Integer.parseInt(p.getProperty("gathertime")));
			//用于ob删除数据
			DataDelete datadelete = new DataDelete(Integer.parseInt(p.getProperty("orders")) * Integer.parseInt(p.getProperty("scalFactor")),
					Integer.parseInt(p.getProperty("workload")),
					Integer.parseInt(p.getProperty("skScalFactor"))*Integer.parseInt(p.getProperty("seckillpay")),
					p.getProperty("ip"), p.getProperty("port"),
					p.getProperty("dbname"), p.getProperty("userName"), p.getProperty("passwd"),
					p.getProperty("driver"), p.getProperty("db"));
			
			datadelete.generate();
			writeLoadGenerator.generate();
		}
		System.out.println("***********************");
		System.out.println("TPS: " + all_tps/count );
		System.out.println("count: " + all_count/count);
		System.out.println("submit TPS: " + submit_tps/count);
		System.out.println("submit count: " + submit_count/count);
		System.out.println("all time(slowest): " + all_time/count + "ms");
		System.out.println("sumit avg response time: " + submittime/count + "us");
		System.out.println("pay avg response time: " + paytime/count + "us");
		System.out.println("cancel avg response time: " + canceltime/count + "us");
		System.out.println("***********************");
	}
}

class ActiveSubmitThreadNum {
	int num;
	public ActiveSubmitThreadNum(int num) {
		super();
		this.num = num;
	}
	public synchronized void sub() {
		num--;
	}
	public int getNum() {
		return num;
	}
}

class OrderSubmitThread implements Runnable {

	private ArrayList<String> orderTrans = null;
	private DBConnector dbConnector = null;
	private int orderkeyStart;

	private CountDownLatch countDownLatch = null;
	private CopyOnWriteArrayList<long[]> responseTimeList = null;

	private Vector<ArrayList<Integer>> custkeyLists = null;
	private ActiveSubmitThreadNum activeThreads = null;
	private int thread_th = 0;

	public OrderSubmitThread(ArrayList<String> orderTrans,
			DBConnector dbConnector, int orderkeyStart,
			CountDownLatch countDownLatch,
			CopyOnWriteArrayList<long[]> responseTimeList,
			Vector<ArrayList<Integer>> custkeyLists,
			ActiveSubmitThreadNum activeThreads, int thread_th) {
		super();
		this.orderTrans = orderTrans;
		this.dbConnector = dbConnector;
		this.orderkeyStart = orderkeyStart;
		this.countDownLatch = countDownLatch;
		this.responseTimeList = responseTimeList;
		this.custkeyLists = custkeyLists;
		this.activeThreads = activeThreads;
		this.thread_th = thread_th;
	}

	public void run() {
		Connection conn = null;
		Statement pst = null;
		PreparedStatement pstmt1 = null, pstmt2 = null, pstmt3 = null, pstmt4 = null;
		long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0, transTime = 0, end_time = 0;
		int failedCount = 0;
		long start_time = System.currentTimeMillis();
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		try {
			conn = dbConnector.getConnection();
			pst = conn.createStatement();
			pst.execute("set @@session.ob_query_timeout=9000000000;");
			pstmt1 = conn.prepareStatement("select sl_price from seckillplan where sl_skpkey = ?");
			pstmt2 = conn.prepareStatement("update seckillplan set sl_skpcount = sl_skpcount + 1 where sl_skpkey = ? and sl_skpcount < sl_plancount");
			pstmt3 = conn.prepareStatement("insert into orders (o_orderkey, o_custkey, o_skpkey, o_price, o_orderdate, o_state) values (?, ?, ?, ?, ?, ?)");
			pstmt4 = conn.prepareStatement("insert into orderitem values (?, ?, 1, ?)");
			ArrayList<Integer> custkeyList = new ArrayList<Integer>();
			int custkeyCount = 0;
			long start = 0;
			long[] responseTime = new long[9];
			for(int i = 0; i < orderTrans.size(); i++) {
				try {
					String[] arr = orderTrans.get(i).split(",");
					int skpkey = Integer.parseInt(arr[0]);
					int itemkey = Integer.parseInt(arr[1]);
					int custkey = Integer.parseInt(arr[2]);

					pstmt1.setInt(1, skpkey);
					start = System.nanoTime();
					ResultSet rs = pstmt1.executeQuery();
					time1 += (System.nanoTime() - start) / 1000;

//					BigDecimal price = null;
					Double price = null;
					if(rs.next()) {
//						price = rs.getBigDecimal(1);
						price = rs.getDouble(1);
					} else {
						System.out.println("select error in SubmitOrderThread!");
						continue;
					}

					conn.setAutoCommit(false);
					long transStart = System.nanoTime();

					pstmt2.setInt(1, skpkey);

					start = System.nanoTime();
					int rows = pstmt2.executeUpdate();
					time2 += (System.nanoTime() - start) / 1000;

					if(rows == 0) {
						conn.commit();
						failedCount++;
						continue;
					}

					pstmt3.setInt(1, orderkeyStart);
					pstmt3.setInt(2, custkey);
					pstmt3.setInt(3, skpkey);
					pstmt3.setDouble(4, price);
//					pstmt3.setBigDecimal(4, price);
//					pstmt3.setDate(5, new Date(new java.util.Date().getTime()));
					pstmt3.setString(5, dateFormater.format(new Date(new java.util.Date().getTime())));
					pstmt3.setInt(6, 0);

					start = System.nanoTime();
					pstmt3.executeUpdate();
					time3 += (System.nanoTime() - start) / 1000;

					pstmt4.setInt(1, orderkeyStart);
					pstmt4.setInt(2, itemkey);
					pstmt4.setDouble(3, price);
//					pstmt4.setBigDecimal(3, price);

					start = System.nanoTime();
					pstmt4.executeUpdate();
					time4 += (System.nanoTime() - start) / 1000;

					start = System.nanoTime();
					conn.commit();
					time5 += (System.nanoTime() - start) / 1000;
					
					transTime += (System.nanoTime() - transStart) / 1000;

					orderkeyStart++;

					custkeyList.add(custkey);
					custkeyCount++;
					if(custkeyCount == orderTrans.size() / 50) {
						custkeyLists.add(custkeyList);
						custkeyList = new ArrayList<Integer>();
						custkeyCount = 0;
					}

				} catch (Exception e) {
					conn.rollback();
					failedCount++;
					e.printStackTrace();
				} finally {
					conn.setAutoCommit(true);
				}
				responseTime [0] = time1;
				responseTime [1] = time2;
				responseTime [2] = time3;
				responseTime [3] = time4;
				responseTime [4] = time5;
				responseTime [5] = transTime;
				responseTime [6] = orderTrans.size();
				responseTime [7] = failedCount;
				responseTime [8] = end_time;
				responseTimeList.set(thread_th, responseTime);
			}
//			end_time = (System.currentTimeMillis() - start_time);
			custkeyLists.add(custkeyList);
//			long [] responseTime = {time1, time2, time3, time4, time5, transTime, orderTrans.size(), failedCount, end_time};
//			System.out.println(Arrays.toString(responseTime));
//			responseTimeList.add(responseTime);
			countDownLatch.countDown();
			activeThreads.sub();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(pstmt1 != null)	pstmt1.close();
				if(pstmt2 != null)	pstmt2.close();
				if(pstmt3 != null)	pstmt3.close();
				if(pstmt4 != null)	pstmt4.close();
				if(conn != null)	conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}


class OrderHandleThread implements Runnable {

	private DBConnector dbConnector = null;
	private int orderHandleDelayTime;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	private Vector<ArrayList<Integer>> custkeyLists = null;
	private ActiveSubmitThreadNum activeThreads = null;
	private int limitSize;
	private CountDownLatch countDownLatch = null;
	private CopyOnWriteArrayList<long[]> responseTimeList = null;
	public static boolean [] lock = new boolean[0];
	private int thread_th = 0;

	public OrderHandleThread(DBConnector dbConnector, int orderHandleDelayTime,
			float payRatio, float cancelRatio, float queryRatio,
			Vector<ArrayList<Integer>> custkeyLists,
			ActiveSubmitThreadNum activeThreads, int limitSize,
			CountDownLatch countDownLatch,
			CopyOnWriteArrayList<long[]> responseTimeList, 
			int thread_th) {
		super();
		this.dbConnector = dbConnector;
		this.orderHandleDelayTime = orderHandleDelayTime;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		this.custkeyLists = custkeyLists;
		this.activeThreads = activeThreads;
		this.limitSize = limitSize;
		this.countDownLatch = countDownLatch;
		this.responseTimeList = responseTimeList;
		this.thread_th = thread_th;
	}

	public void run() {
		Connection conn = null;
		PreparedStatement pstmt1 = null, pstmt2 = null, pstmt3 = null, pstmt4 = null, 
		pstmt5 = null, pstmt6 = null, pstmt7 = null, pstmt8 = null, pstmt9 = null;

		Statement pst = null;
		Random random = new Random();
		long[] responseTime = new long[29];
		int count1 = 0, count2 = 0, count3 = 0, count4 = 0, count5 = 0, count6 = 0, count7 = 0, 
		count8 = 0, count9 = 0, count10 = 0, count11 = 0, count12 = 0;
		long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0, time6 = 0, time7 = 0, 
		time8 = 0, time9 = 0, time10 = 0, time11 = 0, time12 = 0;
		long transTime1 = 0, transTime2 = 0, filedTrans = 0;
		long transCount1 = 0, transCount2 = 0;
		float paycancelratio;
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		
		try {

			Thread.sleep(orderHandleDelayTime);

			conn = dbConnector.getConnection();
			pst = conn.createStatement();
			pst.execute("set @@session.ob_query_timeout=9000000000;");
			pstmt1 = conn.prepareStatement("select * from orders where o_custkey = ? limit " + limitSize);
			pstmt2 = conn.prepareStatement("select * from orders where o_orderkey = ?");
			pstmt3 = conn.prepareStatement("select * from orderitem where oi_orderkey = ?");
			pstmt4 = conn.prepareStatement("select * from orders where o_custkey = ? and o_state = 0");
			pstmt5 = conn.prepareStatement("select o_price from orders where o_orderkey = ?");
			//o_state = 0 真实环境中可以没有?
			pstmt6 = conn.prepareStatement("update orders set o_state = 1,o_paydate = ? where o_orderkey = ? and o_state = 0");
			pstmt7 = conn.prepareStatement("update orders set o_state = 2 where o_orderkey = ? and o_state = 0");
			pstmt8 = conn.prepareStatement("update seckillpay set sa_paycount = sa_paycount + 1 where sa_skpkey = ?");
			pstmt9 = conn.prepareStatement("update seckillplan set sl_skpcount = sl_skpcount - 1 where sl_skpkey = ?");

			ArrayList<Integer> custkeyList = null;

			while(true) {
				if(custkeyLists.size() > 0) {
					synchronized(lock)
					{
						if(custkeyLists.size() > 0) 
						{
							custkeyList = custkeyLists.remove(0);
						}
						else continue;
					}
					for(int i = 0; i < custkeyList.size(); i++) {
						int custkey = custkeyList.get(i);

						try {
							if(random.nextFloat() < queryRatio) {
								pstmt1.setInt(1, custkey);
								long start = System.nanoTime();
								ResultSet rs = pstmt1.executeQuery();
								time1 += (System.nanoTime() - start) / 1000;
								count1++;

								while(rs.next()) {
									int orderkey = rs.getInt(1);

									pstmt2.setInt(1, orderkey);
									start = System.nanoTime();
									pstmt2.executeQuery();
									time2 += (System.nanoTime() - start) / 1000;
									count2++;

									pstmt3.setInt(1, orderkey);
									start = System.nanoTime();
									pstmt3.executeQuery();
									time3 += (System.nanoTime() - start) / 1000;
									count3++;
								}
							}
							paycancelratio = random.nextFloat();
							if( paycancelratio < payRatio) {
								pstmt4.setInt(1, custkey);

								long start = System.nanoTime();
								ResultSet rs1 = pstmt4.executeQuery();
								time4 += (System.nanoTime() - start) / 1000;
								count4++;

								while(rs1.next()) {
									int orderkey = rs1.getInt(1);
									int skpkey = rs1.getInt(3);

									pstmt5.setInt(1, orderkey);
									start = System.nanoTime();
									ResultSet rs2 = pstmt5.executeQuery();
									time5 += (System.nanoTime() - start) / 1000;
									count5++;

									if(rs2.next()) {
										//BigDecimal price = rs2.getBigDecimal(1);
										//这里去和支付系统交互了，这也是为什么pstmt5中有price属性

										long transTime = System.nanoTime();
										conn.setAutoCommit(false);

										pstmt6.setString(1, dateFormater.format(new Date(new java.util.Date().getTime())));
										pstmt6.setInt(2, orderkey);
										start = System.nanoTime();
										int rows = pstmt6.executeUpdate();
										time6 += (System.nanoTime() - start) / 1000;
										count6++;

										if(rows == 0) {
											start = System.nanoTime();
											conn.commit();
											time8 += (System.nanoTime() - start) / 1000;
											count8++;
											continue;
										}

										pstmt8.setInt(1, skpkey);
										start = System.nanoTime();
										pstmt8.executeUpdate();
										time7 += (System.nanoTime() - start) / 1000;
										count7++;

										start = System.nanoTime();
										conn.commit();
										time8 += (System.nanoTime() - start) / 1000;
										count8++;

										transCount1++;
										transTime1 += (System.nanoTime() - transTime) / 1000;

									} else {
										System.out.println("select error in OrderHandleThread(pay)!");
									}
								}
							} 
							else if(paycancelratio < (payRatio+cancelRatio)) 
							{
								pstmt4.setInt(1, custkey);

								long start = System.nanoTime();
								ResultSet rs = pstmt4.executeQuery();
								time9 += (System.nanoTime() - start) / 1000;
								count9++;

								while(rs.next()) {
									int orderkey = rs.getInt(1);
									int skpkey = rs.getInt(3);

									long transTime = System.nanoTime();
									conn.setAutoCommit(false);

									pstmt7.setInt(1, orderkey);
									start = System.nanoTime();
									int rows = pstmt7.executeUpdate();
									time10 += (System.nanoTime() - start) / 1000;
									count10++;

									if(rows == 0) {
										start = System.nanoTime();
										conn.commit();
										time12 += (System.nanoTime() - start) / 1000;
										count12++;
										continue;
									}


									pstmt9.setInt(1, skpkey);
									start = System.nanoTime();
									pstmt9.executeUpdate();
									time11 += (System.nanoTime() - start) / 1000;
									count11++;

									start = System.nanoTime();
									conn.commit();
									time12 += (System.nanoTime() - start) / 1000;
									count12++;

									transCount2++;
									transTime2 += (System.nanoTime() - transTime) / 1000;
								}
							}

						} catch (Exception e) {
							e.printStackTrace();
							conn.rollback();
							filedTrans++;
						} finally {
							conn.setAutoCommit(true);
						}
						responseTime [0] = time1;
						responseTime [1] = count1;
						responseTime [2] = time2;
						responseTime [3] = count2;
						responseTime [4] = time3;
						responseTime [5] = count3;
						responseTime [6] = time4;
						responseTime [7] = count4;
						responseTime [8] = time5;
						responseTime [9] = count5;
						responseTime [10] = time6;
						responseTime [11] = count6;
						responseTime [12] = time7;
						responseTime [13] = count7;
						responseTime [14] = time8;
						responseTime [15] = count8;
						responseTime [16] = time9;
						responseTime [17] = count9;
						responseTime [18] = time10;
						responseTime [19] = count10;
						responseTime [20] = time11;
						responseTime [21] = count11;
						responseTime [22] = time12;
						responseTime [23] = count12;
						responseTime [24] = transTime1;
						responseTime [25] = transCount1;
						responseTime [26] = transTime2;
						responseTime [27] = transCount2;
						responseTime [28] = filedTrans;
						responseTimeList.set(thread_th,responseTime);
					}
				} else if(activeThreads.getNum() != 0){
					Thread.sleep(100);
				} else {
					break;
				}
			}

//			long [] responseTime = {time1, count1, time2, count2, time3, count3, time4, count4, time5, count5, time6, count6, 
//					time7, count7, time8, count8, time9, count9, time10, count10, time11, count11, time12, count12, 
//					transTime1, transCount1, transTime2, transCount2, filedTrans};
//			System.out.println(Arrays.toString(responseTime));
//			responseTimeList.add(responseTime);
			countDownLatch.countDown();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(pstmt1 != null)	pstmt1.close();
				if(pstmt2 != null)	pstmt2.close();
				if(pstmt3 != null)	pstmt3.close();
				if(pstmt4 != null)	pstmt4.close();
				if(pstmt5 != null)	pstmt5.close();
				if(pstmt6 != null)	pstmt6.close();
				if(pstmt7 != null)	pstmt7.close();
				if(pstmt8 != null)	pstmt8.close();
				if(pstmt9 != null)	pstmt9.close();
				if(conn != null)	conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

class QueueLengthControlThread implements Runnable {

	private DBConnector dbConnector = null;
	private int queueLengthControlDelayTime;
	private int cycle;
	private boolean[] flag = null;


	public QueueLengthControlThread(DBConnector dbConnector,
			int queueLengthControlDelayTime, int cycle, boolean[] flag) {
		super();
		this.dbConnector = dbConnector;
		this.queueLengthControlDelayTime = queueLengthControlDelayTime;
		this.cycle = cycle;
		this.flag = flag;
	}

	public void run() {
		Connection conn = null;
		PreparedStatement pstmt1 = null, pstmt2 = null;
		Statement pst = null;
		try {
			Thread.sleep(queueLengthControlDelayTime);
			
			conn = dbConnector.getConnection();
			pst = conn.createStatement();
			pst.execute("set @@session.ob_query_timeout=9000000000;");
			pstmt1 = conn.prepareStatement("select * from seckillplan");
			pstmt2 = conn.prepareStatement("select * from seckillpay");
			int count1 = 0, count2 = 0;
			long time1 = 0, time2 = 0;

			while(true) {
				long start = System.nanoTime();
				pstmt1.executeQuery();
				time1 += (System.nanoTime() - start) / 1000;
				count1++;

				start = System.nanoTime();
				pstmt2.executeQuery();
				time2 += (System.nanoTime() - start) / 1000;
				count2++;

				Thread.sleep(cycle);
				
				if(flag[0]) {
					break;
				}
			}

			System.out.println("avg time1: " + (time1 / count1) + "us");
			System.out.println("avg time2: " + (time2 / count2) + "us");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(pstmt1 != null)	pstmt1.close();
				if(pstmt2 != null)	pstmt2.close();
				if(conn != null)	conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

