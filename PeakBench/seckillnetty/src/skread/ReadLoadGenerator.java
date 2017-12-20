package skread;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import util.DBConnector;
import util.RanDataGene;
import util.ZipfLaw;

public class ReadLoadGenerator {

	private int seckillReadLoad;
	private int nonseckillReadLoad;
	private double[] queryRatio = null;
	private int itemKeyRange;
	private String skItemsFile = null;
	private String typeListFile = null;
	private String supplierListFile = null;
	private int zipfs;
	private int zipfN;
	private int limitSize;
	private int concurrency;
	private String ipFile = null;
	public static float all_time = 0; 
	public static float time1 = 0,time2 = 0,time3 = 0,time4 = 0,time5 = 0;  

	private ArrayList<DBConnector> dbConnector = null;
	private ArrayList<ArrayList<Integer>> skItemsList = null;
	private ArrayList<ArrayList<Integer>> supplierList = null;
	private ArrayList<ArrayList<Integer>> typeList = null;
	private double[] zipfLawData = null;
	public long gathertime = 0;

	public ReadLoadGenerator(int seckillReadLoad, int nonseckillReadLoad,
			double[] queryRatio, int itemKeyRange, String skItemsFile,
			String typeListFile, String supplierListFile, int zipfs, int zipfN,
			int limitSize, int concurrency, String ipFile, int gathertime) {
		super();
		this.seckillReadLoad = seckillReadLoad;
		this.nonseckillReadLoad = nonseckillReadLoad;
		this.queryRatio = queryRatio;
		this.itemKeyRange = itemKeyRange;
		this.skItemsFile = skItemsFile;
		this.typeListFile = typeListFile;
		this.supplierListFile = supplierListFile;
		this.zipfs = zipfs;
		this.zipfN = zipfN;
		this.limitSize = limitSize;
		this.concurrency = concurrency;
		this.ipFile = ipFile;
		this.gathertime = gathertime;
		init();
	}

	@SuppressWarnings("unchecked")
	private void init() 
	{
		double sum = 0;
		for(int i = 0; i < queryRatio.length; i++) {
			sum += queryRatio[i];
		}
		for(int i = 0; i < queryRatio.length; i++) {
			queryRatio[i] = queryRatio[i] / sum;
			if(i != 0) {
				queryRatio[i] += queryRatio[i - 1];
			}
		}
		
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
		int[] skItemsArray = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(skItemsFile)));
			String[] skItemsInfo = br.readLine().split(",");
			skItemsArray = new int[skItemsInfo.length];
			for(int i = 0; i < skItemsInfo.length; i++) {
				skItemsArray[i] = Integer.parseInt(skItemsInfo[i]);
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
		skItemsList = new ArrayList<ArrayList<Integer>>();
		Random random = new Random();
		for(int i = 0; i < zipfN; i++)
			skItemsList.add(new ArrayList<Integer>());
		for(int i = 0; i < skItemsArray.length; i++)
			skItemsList.get(random.nextInt(zipfN)).add(skItemsArray[i]);

		ObjectInputStream ois1 = null, ois2 = null;
		try {
			ois1 = new ObjectInputStream(new FileInputStream(supplierListFile));
			ois2 = new ObjectInputStream(new FileInputStream(typeListFile));
			supplierList = (ArrayList<ArrayList<Integer>>)ois1.readObject();
			typeList = (ArrayList<ArrayList<Integer>>)ois2.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(ois1 != null)	ois1.close();
				if(ois2 != null)	ois2.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		ZipfLaw<Integer> zipfLaw = new ZipfLaw<Integer>(zipfs, zipfN);
		zipfLawData = zipfLaw.getZipfLawData();
	}

	class ReadLoadThread implements Runnable {
		
		private DBConnector dbConnector_i = null;
		private Connection conn = null;
		private PreparedStatement pstmt1 = null, pstmt2 = null, pstmt3 = null, pstmt4 = null, pstmt5 = null;
		private Statement pst = null;
		private int thread_th = 0;
		
		private CountDownLatch countDownLatch = null;
		private CopyOnWriteArrayList<long[]> responseTimeList = null;

		public ReadLoadThread(CountDownLatch countDownLatch,
				CopyOnWriteArrayList<long[]> responseTimeList, DBConnector dbConnector_i, int thread_th) {
			super();
			this.countDownLatch = countDownLatch;
			this.responseTimeList = responseTimeList;
			this.dbConnector_i = dbConnector_i;
			this.thread_th = thread_th;
		}

		public void run() {
			try {
				conn = dbConnector_i.getConnection();
				pst = conn.createStatement();
			    pst.execute("set @@session.ob_query_timeout=9000000000;");
				pstmt1 = conn.prepareStatement("select * from item where itemkey = ?");
				pstmt2 = conn.prepareStatement("select * from item where type = ? limit " + limitSize);
				pstmt3 = conn.prepareStatement("select * from item where type = ? and name like ? limit " + limitSize);
				pstmt4 = conn.prepareStatement("select * from item where type = ? and price between ? and ? limit " + limitSize);
				pstmt5 = conn.prepareStatement("select * from item where suppkey = ? limit " + limitSize);
				long[] responseTime = new long[10];
				int count1 = 0, count2 = 0, count3 = 0, count4 = 0, count5 = 0;
				long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0;
				int readLoad = (seckillReadLoad + nonseckillReadLoad) / concurrency;
				float skLoadRatio = (float)seckillReadLoad / (seckillReadLoad + nonseckillReadLoad);
				Random random = new Random();

				for(int i = 0; i < readLoad; i++) {
					Float randomValue = random.nextFloat();
					if(randomValue < queryRatio[0]) {
						int itemkey = 0;
						if(random.nextFloat() <= skLoadRatio) {
							itemkey = getZipfLawValue(random, skItemsList);
						} else {
							itemkey = random.nextInt(itemKeyRange);
						}
						pstmt1.setInt(1, itemkey);
						long start = System.nanoTime();
						pstmt1.executeQuery();
						time1 += (System.nanoTime() - start) / 1000;
						count1++;
					} else if(randomValue < queryRatio[1]) {
						pstmt2.setInt(1, getZipfLawValue(random, typeList));
						long start = System.nanoTime();
						pstmt2.executeQuery();
						time2 += (System.nanoTime() - start) / 1000;
						count2++;
					} else if(randomValue < queryRatio[2]) {
						pstmt3.setInt(1, getZipfLawValue(random, typeList));
						pstmt3.setString(2, RanDataGene.getChar() + RanDataGene.getChar() + "%");
						long start = System.nanoTime();
						pstmt3.executeQuery();
						time3 += (System.nanoTime() - start) / 1000;
						count3++;
					} else if(randomValue < queryRatio[3]) {
						float price1 = RanDataGene.getInteger(1, 500) - random.nextFloat();
						float price2 = price1 + RanDataGene.getInteger(1, 500);
						pstmt4.setInt(1, getZipfLawValue(random, typeList));
						pstmt4.setFloat(2, price1);
						pstmt4.setFloat(3, price2);
						long start = System.nanoTime();
						pstmt4.executeQuery();
						time4 += (System.nanoTime() - start) / 1000;
						count4++;
					} else {
						pstmt5.setInt(1, getZipfLawValue(random, supplierList));
						long start = System.nanoTime();
						pstmt5.executeQuery();
						time5 += (System.nanoTime() - start) / 1000;
						count5++;
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
					responseTimeList.set(thread_th ,responseTime);
				}
				//long[] responseTime = {time1, count1, time2, count2, time3, count3, time4, count4, time5, count5};
				//System.out.println(Arrays.toString(responseTime));
				//responseTimeList.add(responseTime);
				
				countDownLatch.countDown();
			} catch (SQLException e) {
				e.printStackTrace();
			}catch (FileNotFoundException e){
				e.printStackTrace();
			} finally {
				try {
					if(pstmt1 != null)	pstmt1.close();
					if(pstmt2 != null)	pstmt2.close();
					if(pstmt3 != null)	pstmt3.close();
					if(pstmt4 != null)	pstmt4.close();
					if(pstmt5 != null)	pstmt5.close();
					if(conn != null)	conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		private int getZipfLawValue(Random random, ArrayList<ArrayList<Integer>> list) {
			Float randomValue = random.nextFloat();
			int result = 0;
			for(int i = 0; i < zipfLawData.length; i++) {
				if(randomValue <= zipfLawData[i]) {
					if(list.get(i).size() == 0) {
						randomValue = random.nextFloat();
						i = -1;
						continue;
					}
					result = list.get(i).get(random.nextInt(list.get(i).size()));
					break;
				}
			}
			return result;
		}
	}

	public void generate() {
		long start = System.currentTimeMillis();
		final CountDownLatch countDownLatch = new CountDownLatch(concurrency+1);
		final CopyOnWriteArrayList<long[]> responseTimeList = new CopyOnWriteArrayList<long[]>();
		for(int i = 0; i < concurrency; i++) {
			responseTimeList.add(new long[0]);
		}
		
		//System.out.println("info of all threads[time1, count1, time2, count2, time3, count3, time4, count4, time5, count5]:");
		for(int i = 0; i < concurrency; i++) {
			new Thread(new ReadLoadThread(countDownLatch, responseTimeList, dbConnector.get(i%dbConnector.size()), i)).start();
		}
		System.out.println("thread end~~");
		new Timer().schedule(new TimerTask() {   
			long time = 0, count = 0;
            public void run() {  
                // TODO Auto-generated method stub  
                System.out.println("bombing!"); 
                for(int i = 0; i < 10; i+=2) {
        			time = 0; count = 0;
        			for(int j = 0; j < 200 ; j++) {
        				time += responseTimeList.get(j)[i];
        				count += responseTimeList.get(j)[i + 1];
        			}
        			if((i / 2 + 1)==1)
        				time1+= ((float)time / count);
        			else if((i / 2 + 1)==2)
        				time2+= ((float)time / count);
        			else if((i / 2 + 1)==3)
        				time3+= ((float)time / count);
        			else if((i / 2 + 1)==4)
        				time4+= ((float)time / count);
        			else if((i / 2 + 1)==5)
        				time5+= ((float)time / count);
        			System.out.println("read " + (i / 2 + 1) + " avg response time: " + ((float)time / count) + "us");
        		}
                System.out.println("read QPS : " + ( count * 1000 / gathertime) );
                if(countDownLatch.getCount() == 1)
                {
                	cancel();
                	countDownLatch.countDown();
                }
            }  
        }, 5000, gathertime);
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		all_time += (System.currentTimeMillis() - start);
		System.out.println("all time(slowest): " + (System.currentTimeMillis() - start) + "ms");
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
		String[] queryRatios = p.getProperty("queryRatio").split(",");
		double[] queryRatio = new double[5];
		queryRatio[0] = Double.parseDouble(queryRatios[0].trim());
		queryRatio[1] = Double.parseDouble(queryRatios[1].trim());
		queryRatio[2] = Double.parseDouble(queryRatios[2].trim());
		queryRatio[3] = Double.parseDouble(queryRatios[3].trim());
		queryRatio[4] = Double.parseDouble(queryRatios[4].trim());
		ReadLoadGenerator readLoadGenerator = new ReadLoadGenerator(Integer.parseInt(p.getProperty("seckillReadLoad")), 
				Integer.parseInt(p.getProperty("nonseckillReadLoad")), queryRatio, 
				Integer.parseInt(p.getProperty("itemKeyRange")), p.getProperty("skItemsFile"),
				p.getProperty("typeListFile"), p.getProperty("supplierListFile"),  
				Integer.parseInt(p.getProperty("zipfs")),Integer.parseInt(p.getProperty("zipfN")), 
				Integer.parseInt(p.getProperty("limitSize")),Integer.parseInt(p.getProperty("ReadLoadGenerator.concurrency")), 
				p.getProperty("ipfile"), Integer.parseInt(p.getProperty("gathertime")));
		int count = 10, i = 0;
		while(i++ < count)
		{
			readLoadGenerator.generate();
		}
		System.out.println("QPS(slowest): " + (Integer.parseInt(p.getProperty("seckillReadLoad")) + 
				Integer.parseInt(p.getProperty("nonseckillReadLoad"))) * count / all_time );
		System.out.println("all time(slowest): " + all_time/count + "ms");
		System.out.println("read 1 avg response time: " + time1/count + "us");
		System.out.println("read 1 avg response time: " + time2/count + "us");
		System.out.println("read 1 avg response time: " + time3/count + "us");
		System.out.println("read 1 avg response time: " + time4/count + "us");
		System.out.println("read 1 avg response time: " + time5/count + "us");
	}
}



