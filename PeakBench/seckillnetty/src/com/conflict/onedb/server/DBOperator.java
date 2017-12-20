package com.conflict.onedb.server;

import java.io.FileNotFoundException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.log4j.Logger;
import com.conflict.onedb.client.MessageProto;

import util.DBConnector;
import util.Operation;
import util.RanDataGene;
import util.SeckillItem;
import util.otherTask;

public class DBOperator implements Runnable {
	private int threadNum, MastercontrolNum;
	private int queueSize;
	private DBConnector dbConnector = null;
	private ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>> sckoperationQueues = null;
	private ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>> sckotheroperationQueues = null;
	private ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>> sckreadoperationQueues = null;
	private ArrayBlockingQueue<DBOperatorThread> waitQueue = null;
	private CopyOnWriteArrayList<long[]> readresponseTimeList = new CopyOnWriteArrayList<long[]>();
	private CopyOnWriteArrayList<long[]> writeresponseTimeList = new CopyOnWriteArrayList<long[]>();
	private CopyOnWriteArrayList<long[]> readclientresponseTimeList = new CopyOnWriteArrayList<long[]>();
	private CopyOnWriteArrayList<long[]> writeclientresponseTimeList = new CopyOnWriteArrayList<long[]>();
	public long gathertime = 0;
	public int readtimestamp = 0, writetimestamp = 0, readwriteinterval = 0, baseNum;
	public boolean hashflag = true;
	public OtherOperator otherOperator = null;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	private int writeDataGeneratorType, conflictWorkloadcount;
	private int readDataGeneratorType, readWorkloadcount;
	private static Logger logger = Logger.getLogger("infoLogger");
	public static MessageProto.MessageWrite sckmessage = null;
	private CountDownLatch countDownLatch, threadcountDownLatch;
	private int testPayimpact, testPaysck, DBType, cacheType, hashMapType;
	private String writeotherworkloadcountFile = null;
	private ArrayList<SeckillItem> dbData = null;
	private ArrayList<HashMap<Integer, Integer>> dbDataList = null;
	private HashMap<Integer, ArrayList<Integer>> hashselect = new HashMap<Integer, ArrayList<Integer>>();
//	private static Random random = new Random();

	public DBOperator(int threadNum, int MastercontrolNum, int queueSize, DBConnector dbConnector, long gathertime, 
			int readtimestamp, int writetimestamp, int readwriteinterval, float payRatio, float cancelRatio, 
			float queryRatio, int writeDataGeneratorType, ArrayList<SeckillItem> dbData,
			int baseNum, int conflictWorkloadcount, int readDataGeneratorType,
			int readWorkloadcount, int testPayimpact, int testPaysck, int DBType, 
			String writeotherworkloadcountFile, int cacheType, int hashMapType) {
		super();
		this.threadNum = threadNum;
		this.MastercontrolNum = MastercontrolNum;
		this.queueSize = queueSize;
		this.dbConnector = dbConnector;
		this.gathertime = gathertime;
		this.readtimestamp = readtimestamp;
		this.writetimestamp = writetimestamp;
		this.readwriteinterval = readwriteinterval;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		this.writeDataGeneratorType = writeDataGeneratorType;
		this.dbData = dbData;
		this.baseNum = baseNum;
		this.conflictWorkloadcount = conflictWorkloadcount;
		this.readDataGeneratorType = readDataGeneratorType;
		this.readWorkloadcount = readWorkloadcount;
		this.testPayimpact = testPayimpact;
		this.testPaysck = testPaysck;
		this.DBType = DBType;
		this.writeotherworkloadcountFile = writeotherworkloadcountFile;
		this.cacheType = cacheType;
		this.hashMapType = hashMapType;
	}

	public void run() {
		if(cacheType == 0){
			sckoperationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>>(MastercontrolNum);
			sckotheroperationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>>(MastercontrolNum);
			sckreadoperationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>>(MastercontrolNum);
			for (int i = 0; i < MastercontrolNum; i++)
			{
				sckoperationQueues.add(new ArrayBlockingQueue<Operation<MessageProto.Message>>(1000000));
				sckotheroperationQueues.add(new ArrayBlockingQueue<Operation<MessageProto.Message>>(1000000));
				sckreadoperationQueues.add(new ArrayBlockingQueue<Operation<MessageProto.Message>>(1000000));
			}
			waitQueue = new ArrayBlockingQueue<DBOperatorThread>(threadNum * 10);
		}else if(cacheType == 1)
		{
			sckoperationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>>(threadNum);
			sckotheroperationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>>(threadNum);
			sckreadoperationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>>(threadNum);
			for (int i = 0; i < threadNum; i++)
			{
				sckoperationQueues.add(new ArrayBlockingQueue<Operation<MessageProto.Message>>(queueSize));
				sckotheroperationQueues.add(new ArrayBlockingQueue<Operation<MessageProto.Message>>(queueSize));
				sckreadoperationQueues.add(new ArrayBlockingQueue<Operation<MessageProto.Message>>(queueSize));
			}
		}
		if(hashMapType == 1)
		{
			dbDataList = new ArrayList<HashMap<Integer, Integer>>(threadNum);
			for(int i = 0; i < threadNum; i++){
				dbDataList.add(new HashMap<Integer, Integer>());
			}
			createHashmap();
//			for(int i = 0; i < threadNum; i++){
//				System.out.println("!!!!!!!!*********"+dbDataList.get(i));
//			}
		}
		for (int i = 0; i < threadNum; i++) {
			readresponseTimeList.add(new long[0]);
			writeresponseTimeList.add(new long[0]);
			readclientresponseTimeList.add(new long[0]);
			writeclientresponseTimeList.add(new long[0]);
		}
		if (writetimestamp != 0 || (payRatio + cancelRatio + queryRatio) > 0) {
			if(writetimestamp==0){
				otherOperator = new OtherOperator(200, readwriteinterval, writeDataGeneratorType,
						writeotherworkloadcountFile);
			}			
			else{
				otherOperator = new OtherOperator(writetimestamp, readwriteinterval, writeDataGeneratorType,
						writeotherworkloadcountFile);
			}
			new Thread(otherOperator).start();
		}

		// try {
		// Thread.sleep(readwriteinterval);
		// } catch (InterruptedException e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }
		long allTimeStart = 0;
		if(cacheType == 0)
		{
			for (int i = 0; i < threadNum; i++) {
				try {
					new Thread(new DBOperatorThread(waitQueue, readresponseTimeList, readclientresponseTimeList,
							writeresponseTimeList, writeclientresponseTimeList, dbConnector.getNewConnection(), i,
							otherOperator, writetimestamp, payRatio, cancelRatio, queryRatio, testPayimpact, testPaysck,
							writeDataGeneratorType, DBType, cacheType, hashMapType)).start();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
			countDownLatch = new CountDownLatch(MastercontrolNum);
			allTimeStart = System.nanoTime();
			for(int j = 0; j < MastercontrolNum; j++)
			{
				new Thread(new MastercontrolThread(sckreadoperationQueues.get(j), sckoperationQueues.get(j), sckotheroperationQueues.get(j),
						waitQueue, writeDataGeneratorType, conflictWorkloadcount, readDataGeneratorType, readWorkloadcount,
						countDownLatch)).start();
			}
		}
		else if(cacheType == 1){
			threadcountDownLatch = new CountDownLatch(threadNum);
			allTimeStart = System.nanoTime();
			for (int i = 0; i < threadNum; i++) {
				try {
					if(hashMapType == 1){
						new Thread(new DBOperatorThread(waitQueue, readresponseTimeList, readclientresponseTimeList,
								writeresponseTimeList, writeclientresponseTimeList, dbConnector.getNewConnection(), i,
								otherOperator, writetimestamp, payRatio, cancelRatio, queryRatio, testPayimpact, testPaysck,
								writeDataGeneratorType, DBType, cacheType, threadcountDownLatch, hashMapType, dbDataList.get(i),
								sckreadoperationQueues.get(i), sckoperationQueues.get(i), sckotheroperationQueues.get(i))).start();
					}
					else{
						new Thread(new DBOperatorThread(waitQueue, readresponseTimeList, readclientresponseTimeList,
								writeresponseTimeList, writeclientresponseTimeList, dbConnector.getNewConnection(), i,
								otherOperator, writetimestamp, payRatio, cancelRatio, queryRatio, testPayimpact, testPaysck,
								writeDataGeneratorType, DBType, cacheType, threadcountDownLatch, hashMapType, null,
								sckreadoperationQueues.get(i), sckoperationQueues.get(i), sckotheroperationQueues.get(i))).start();
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
		new Timer().schedule(new TimerTask() {
			MessageReturnProto.MessageReturn.Builder workMessage = MessageReturnProto.MessageReturn.newBuilder();
			long secktime = 0, seckcount = 0, seckallcount = 0, secklastallcount = 0, seckclienttime = 0;
			long nosecktime = 0, noseckcount = 0, noseckallcount = 0, nosecklastallcount = 0;
			long seckallfailcount = 0, secklastallfailcount = 0, noseckallfailcount = 0, nosecklastallfailcount = 0,
					seckallfailqueuecount = 0, secklastallfailqueuecount = 0, secksbmitcount = 0, seckpaycount = 0, 
					seckslectcount = 0, seckcancelcount = 0;
			long time = 0, count = 0, allcount = 0, lastallcount = 0, clienttime = 0;
			int timestamp = 0;
			long Rtimelast1=0,Rtimelast2=0,Rtimelast3=0,Rtimelast4=0,Rtimelast5=0,Rtimelast6=0,
					Rctimelast1=0,Rctimelast2=0,Rctimelast3=0,Rctimelast4=0,Rctimelast5=0,Rctimelast6=0,
					Wtimelast1=0,Wtimelast2=0,Wtimelast3=0,Wtimelast4=0,Wctimelast1=0,Wctimelast2=0,Wctimelast3=0,Wctimelast4=0;
			long Rcountlast1=0,Rcountlast2=0,Rcountlast3=0,Rcountlast4=0,Rcountlast5=0,Rcountlast6=0,
					Wcountlast1=0,Wcountlast2=0,Wcountlast3=0,Wcountlast4=0;
					
			public void run() {
				workMessage.setMsgID(timestamp++);
				workMessage.setTimestamp(System.currentTimeMillis());
				for (int i = 0; i < 12; i += 2) {
					time = 0;
					clienttime = 0;
					count = 0;
					for (int j = 0; j < threadNum; j++) {
						if (readresponseTimeList.get(j).length != 0) {
							time += readresponseTimeList.get(j)[i];
							clienttime += readclientresponseTimeList.get(j)[i]/1000;
							count += readresponseTimeList.get(j)[i + 1];
						}
					}

					if ((i / 2 + 1) == 1) {
						if ((count-Rcountlast1) != 0) {
							workMessage.setRtime1(((float) (time - Rtimelast1) / (count-Rcountlast1)));
							System.out.println("!!!!!!!!*******read time1**" + (float) (time- Rtimelast1) / (count-Rcountlast1) + "*****");
							logger.info("!!!!!!!!*******read time1**" + (float) (time- Rtimelast1) / (count-Rcountlast1) + "*****");
							Rtimelast1 = time;
							workMessage.setRctime1(((float) (clienttime - Rctimelast1) / (count-Rcountlast1)));
							System.out.println("!!!!!!!!*******read client time1**" + (float) (clienttime - Rctimelast1) / (count-Rcountlast1) + "*****");
							logger.info("!!!!!!!!*******read client time1**" + (float) (clienttime - Rctimelast1) / (count-Rcountlast1) + "*****");
							Rctimelast1 = clienttime;
							Rcountlast1 = count;
						} else{
							workMessage.setRtime1(0);
							workMessage.setRctime1(0);
						}
					} else if ((i / 2 + 1) == 2) {
						if ((count-Rcountlast2) != 0) {
							workMessage.setRtime2(((float) (time- Rtimelast2) / (count-Rcountlast2)));
							System.out.println("!!!!!!!!*******read time2**" + (float) (time- Rtimelast2) / (count-Rcountlast2) + "*****");
							logger.info("!!!!!!!!*******read time2**" + (float) (time- Rtimelast2) / (count-Rcountlast2) + "*****");
							Rtimelast2 = time;
							workMessage.setRctime2(((float) (clienttime - Rctimelast2) / (count-Rcountlast2)));
							System.out.println("!!!!!!!!*******read client time2**" + (float) (clienttime - Rctimelast2) / (count-Rcountlast2) + "*****");
							logger.info("!!!!!!!!*******read client time2**" + (float) (clienttime - Rctimelast2) / (count-Rcountlast2) + "*****");
							Rctimelast2 = clienttime;
							Rcountlast2 = count;
						} else{
							workMessage.setRtime2(0);
							workMessage.setRctime2(0);
						}
					} else if ((i / 2 + 1) == 3) {
						if ((count-Rcountlast3) != 0) {
							workMessage.setRtime3(((float) (time- Rtimelast3) / (count-Rcountlast3)));
							System.out.println("!!!!!!!!*******read time3**" + (float) (time- Rtimelast3) / (count-Rcountlast3) + "*****");
							logger.info("!!!!!!!!*******read time3**" + (float) (time- Rtimelast3) / (count-Rcountlast3) + "*****");
							Rtimelast3 = time;
							workMessage.setRctime3(((float) (clienttime - Rctimelast3) / (count-Rcountlast3)));
							System.out.println("!!!!!!!!*******read client time3**" + (float) (clienttime - Rctimelast3) / (count-Rcountlast3) + "*****");
							logger.info("!!!!!!!!*******read client time3**" + (float) (clienttime - Rctimelast3) / (count-Rcountlast3) + "*****");
							Rctimelast3 = clienttime;
							Rcountlast3 = count;
						} else{
							workMessage.setRtime3(0);
							workMessage.setRctime3(0);
						}
					} else if ((i / 2 + 1) == 4) {
						if ((count-Rcountlast4) != 0) {
							workMessage.setRtime4(((float) (time- Rtimelast4) / (count-Rcountlast4)));
							System.out.println("!!!!!!!!*******read time4**" + (float) (time- Rtimelast4) / (count-Rcountlast4) + "*****");
							logger.info("!!!!!!!!*******read time4**" + (float) (time- Rtimelast4) / (count-Rcountlast4) + "*****");
							Rtimelast4 = time;
							workMessage.setRctime4(((float) (clienttime - Rctimelast4) / (count-Rcountlast4)));
							System.out.println("!!!!!!!!*******read client time4**" + (float) (clienttime - Rctimelast4) / (count-Rcountlast4) + "*****");
							logger.info("!!!!!!!!*******read client time4**" + (float) (clienttime - Rctimelast4) / (count-Rcountlast4) + "*****");
							Rctimelast4 = clienttime;
							Rcountlast4 = count;
						} else{
							workMessage.setRtime4(0);
							workMessage.setRctime4(0);
						}
					} else if ((i / 2 + 1) == 5) {
						if ((count-Rcountlast5) != 0) {
							workMessage.setRtime5(((float) (time- Rtimelast5) / (count-Rcountlast5)));
							System.out.println("!!!!!!!!*******read time5**" + (float) (time- Rtimelast5) / (count-Rcountlast5) + "*****");
							logger.info("!!!!!!!!*******read time5**" + (float) (time- Rtimelast5) / (count-Rcountlast5) + "*****");
							Rtimelast5 = time;
							workMessage.setRctime5(((float) (clienttime - Rctimelast5) / (count-Rcountlast5)));
							System.out.println("!!!!!!!!*******read client time5**" + (float) (clienttime - Rctimelast5) / (count-Rcountlast5) + "*****");
							logger.info("!!!!!!!!*******read client time5**" + (float) (clienttime - Rctimelast5) / (count-Rcountlast5) + "*****");
							Rctimelast5 = clienttime;
							Rcountlast5 = count;
						} else{
							workMessage.setRtime5(0);
							workMessage.setRctime5(0);
						}
					} else if ((i / 2 + 1) == 6) {
						if ((count-Rcountlast6) != 0) {
							workMessage.setRtime6(((float) (time- Rtimelast6) / (count-Rcountlast6)));
							System.out.println("!!!!!!!!*******read time6**" + (float) (time- Rtimelast6) / (count-Rcountlast6) + "*****");
							logger.info("!!!!!!!!*******read time6**" + (float) (time- Rtimelast6) / (count-Rcountlast6) + "*****");
							Rtimelast6 = time;
							workMessage.setRctime6(((float) (clienttime - Rctimelast6) / (count-Rcountlast6)));
							System.out.println("!!!!!!!!*******read client time6**" + (float) (clienttime - Rctimelast6) / (count-Rcountlast6) + "*****");
							logger.info("!!!!!!!!*******read client time6**" + (float) (clienttime - Rctimelast6) / (count-Rcountlast6) + "*****");
							Rctimelast6 = clienttime;
							Rcountlast6 = count;
						} else{
							workMessage.setRtime6(0);
							workMessage.setRctime6(0);
						}
					}
					// System.out.println("!!!*******"+count+"*****");
					allcount += count;
				}
				// System.out.println("*******"+(allcount -
				// lastallcount)+"*****");
				System.out.println("!!!!!!!!*******read:**" + (float) (allcount - lastallcount) * 1000 / gathertime + "*****");
				logger.info("!!!!!!!!*******read:**" + (float) (allcount - lastallcount) * 1000 / gathertime + "*****");
				workMessage.setQps((float) (allcount - lastallcount) * 1000 / gathertime);

				for (int i = 0; i < 8; i += 2) {
					secktime = 0;
					seckcount = 0;
					seckclienttime = 0;
					for (int j = 0; j < threadNum; j++) {
						if (writeresponseTimeList.get(j).length != 0) {
							seckcount += writeresponseTimeList.get(j)[i];
							secktime += writeresponseTimeList.get(j)[i + 1];
							seckclienttime += writeclientresponseTimeList.get(j)[i + 1]/1000;
						}
					}
					if ((i / 2 + 1) == 1) {
						if ((seckcount - Wcountlast1) != 0) {
							workMessage.setWtime1(((float) (secktime - Wtimelast1) / (seckcount - Wcountlast1)));
							System.out.println("!!!!!!!!*******write count1**" + (seckcount - secksbmitcount) + "*****");
							logger.info("!!!!!!!!*******write count1**" + (seckcount - secksbmitcount) + "*****");
							secksbmitcount = seckcount;
							System.out.println("!!!!!!!!*******write time1**" + (float) (secktime - Wtimelast1) / (seckcount - Wcountlast1) + "*****");
							logger.info("!!!!!!!!*******write time1**" + (float) (secktime - Wtimelast1) / (seckcount - Wcountlast1) + "*****");
							Wtimelast1 = secktime;
							workMessage.setWctime1(((float) (seckclienttime - Wctimelast1) / (seckcount - Wcountlast1)));
							System.out.println("!!!!!!!!*******write client time1**" + (float) (seckclienttime - Wctimelast1) / (seckcount - Wcountlast1) + "*****");
							logger.info("!!!!!!!!*******write client time1**" + (float) (seckclienttime - Wctimelast1) / (seckcount - Wcountlast1) + "*****");
							Wctimelast1 = seckclienttime;
							Wcountlast1 = seckcount; 
						} else{
							workMessage.setWtime1(0);
							workMessage.setWctime1(0);
						}
					} else if ((i / 2 + 1) == 2) {
						if ((seckcount - Wcountlast2) != 0) {
							workMessage.setWtime2(((float) (secktime - Wtimelast2) / (seckcount - Wcountlast2)));
							System.out.println("!!!!!!!!*******write count2**" + (seckcount - seckpaycount) + "*****");
							logger.info("!!!!!!!!*******write count2**" + (seckcount - seckpaycount) + "*****");
							seckpaycount = seckcount;
							System.out.println("!!!!!!!!*******write time2**" + (float) (secktime - Wtimelast2) / (seckcount - Wcountlast2) + "*****");
							logger.info("!!!!!!!!*******write time2**" + (float) (secktime - Wtimelast2) / (seckcount - Wcountlast2) + "*****");
							Wtimelast2 = secktime;
							workMessage.setWctime2(((float) (seckclienttime - Wctimelast2) / (seckcount - Wcountlast2)));
							System.out.println("!!!!!!!!*******write client time2**" + (float) (seckclienttime - Wctimelast2) / (seckcount - Wcountlast2) + "*****");
							logger.info("!!!!!!!!*******write client time2**" + (float) (seckclienttime - Wctimelast2) / (seckcount - Wcountlast2) + "*****");
							Wctimelast2 = seckclienttime;
							Wcountlast2 = seckcount;
						} else{
							workMessage.setWtime2(0);
							workMessage.setWctime2(0);
						}
					} else if ((i / 2 + 1) == 3) {
						if ((seckcount - Wcountlast3) != 0) {
							workMessage.setWtime3(((float) (secktime - Wtimelast3) / (seckcount - Wcountlast3)));
							System.out.println("!!!!!!!!*******write count3**" + (seckcount - seckslectcount) + "*****");
							logger.info("!!!!!!!!*******write count3**" + (seckcount - seckslectcount) + "*****");
							seckslectcount = seckcount;
							System.out.println("!!!!!!!!*******write time3**" + (float) (secktime - Wtimelast3) / (seckcount - Wcountlast3) + "*****");
							logger.info("!!!!!!!!*******write time3**" + (float) (secktime - Wtimelast3) / (seckcount - Wcountlast3) + "*****");
							Wtimelast3 = secktime;
							workMessage.setWctime3(((float) (seckclienttime - Wctimelast3) / (seckcount - Wcountlast3)));
							System.out.println("!!!!!!!!*******write client time3**" + (float) (seckclienttime - Wctimelast3) / (seckcount - Wcountlast3) + "*****");
							logger.info("!!!!!!!!*******write client time3**" + (float) (seckclienttime - Wctimelast3) / (seckcount - Wcountlast3) + "*****");
							Wctimelast3 = seckclienttime;
							Wcountlast3 = seckcount;
						} else{
							workMessage.setWtime3(0);
							workMessage.setWctime3(0);
						}
					} else if ((i / 2 + 1) == 4) {
						if ((seckcount - Wcountlast4) != 0) {
							workMessage.setWtime4(((float) (secktime - Wtimelast4) / (seckcount - Wcountlast4)));
							System.out.println("!!!!!!!!*******write count4**" + (seckcount - seckcancelcount) + "*****");
							logger.info("!!!!!!!!*******write count4**" + (seckcount - seckcancelcount) + "*****");
							seckcancelcount = seckcount;
							System.out.println("!!!!!!!!*******write time4**" + (float) (secktime - Wtimelast4) / (seckcount - Wcountlast4) + "*****");
							logger.info("!!!!!!!!*******write time4**" + (float) (secktime - Wtimelast4) / (seckcount - Wcountlast4) + "*****");
							Wtimelast4 = secktime;
							workMessage.setWctime4(((float) (seckclienttime - Wctimelast4) / (seckcount - Wcountlast4)));
							System.out.println("!!!!!!!!*******write client time4**" + (float) (seckclienttime - Wctimelast4) / (seckcount - Wcountlast4) + "*****");
							logger.info("!!!!!!!!*******write client time4**" + (float) (seckclienttime - Wctimelast4) / (seckcount - Wcountlast4) + "*****");
							Wctimelast4 = seckclienttime;
							Wcountlast4 = seckcount;
						} else{
							workMessage.setWtime4(0);
							workMessage.setWctime4(0);
						}
					}
					seckallcount += seckcount;
				}
				
				for (int i = 8; i < 16; i += 2) {
					nosecktime = 0;
					noseckcount = 0;
					for (int j = 0; j < threadNum; j++) {
						if (writeresponseTimeList.get(j).length != 0) {
							noseckcount += writeresponseTimeList.get(j)[i];
							nosecktime += writeresponseTimeList.get(j)[i + 1];
						}
					}
					if ((i / 2 + 1) == 5) {
						if (noseckcount != 0)
							workMessage.setWtime5(((float) nosecktime / noseckcount));
						else
							workMessage.setWtime5(0);
					} else if ((i / 2 + 1) == 6) {
						if (noseckcount != 0)
							workMessage.setWtime6(((float) nosecktime / noseckcount));
						else
							workMessage.setWtime6(0);
					} else if ((i / 2 + 1) == 7) {
						if (noseckcount != 0)
							workMessage.setWtime7(((float) nosecktime / noseckcount));
						else
							workMessage.setWtime7(0);
					} else if ((i / 2 + 1) == 8) {
						if (noseckcount != 0)
							workMessage.setWtime8(((float) nosecktime / noseckcount));
						else
							workMessage.setWtime8(0);
					}
					noseckallcount += noseckcount;
				}
				for (int j = 0; j < threadNum; j++) {
					if (writeresponseTimeList.get(j).length != 0) {
						seckallfailcount += writeresponseTimeList.get(j)[16];
					}
				}
				System.out.println("!!!!!!!!*******write seckallfailcount**" + (seckallfailcount -secklastallfailcount)  + "*****");
				for (int j = 0; j < threadNum; j++) {
					if (writeresponseTimeList.get(j).length != 0) {
						seckallfailqueuecount += writeresponseTimeList.get(j)[17];
					}
				}
				for (int j = 0; j < threadNum; j++) {
					if (writeresponseTimeList.get(j).length != 0) {
						noseckallfailcount += writeresponseTimeList.get(j)[18];
					}
				}
				System.out.println("!!!!!!!!*******write:**"
						+ ((float) (seckallcount + noseckallcount - secklastallcount - nosecklastallcount)) * 1000
								/ gathertime
						+ "*****");
				// System.out.println("*******!!!!!!!!*******"+(seckallfailcount
				// - secklastallfailcount));
				logger.info("!!!!!!!!*******write:**"
						+ ((float) (seckallcount + noseckallcount - secklastallcount - nosecklastallcount)) * 1000
								/ gathertime
						+ "*****");
				// System.out.println("~~~~~~*******"+(seckallcount-secklastallcount)+"*****");
				// System.out.println("%%%%%%%%%%%%%"+gathertime+"*****");
				workMessage.setSecksucesscount(seckallcount - secklastallcount);
				workMessage.setNosecksucesscount(noseckallcount - nosecklastallcount);
				workMessage.setSeckfailcount(seckallfailcount - secklastallfailcount);
				workMessage.setNoseckfailcount(noseckallfailcount - nosecklastallfailcount);
				workMessage.setQueuefailcount(seckallfailqueuecount - secklastallfailqueuecount);
				workMessage.setTps(((float) (seckallcount + noseckallcount - secklastallcount - nosecklastallcount))
						* 1000 / gathertime);

				ReturnNettyClient.routeWorkOrder(workMessage.build());

//				timestamp >( readwriteinterval ==0 ? (readtimestamp==0?writetimestamp:readtimestamp) : (readwriteinterval + writetimestamp)) &&
//				if ( allcount == lastallcount
//						&& seckallcount == secklastallcount && noseckallcount == nosecklastallcount
//						&& seckallfailcount == secklastallfailcount && noseckallfailcount == nosecklastallfailcount
//						&& seckallfailqueuecount == secklastallfailqueuecount && countDownLatch.getCount() == 0
//						&& threadcountDownLatch.getCount() == 0) {
////					System.out.println("*********************aaaaaaaaaaaaaa*****");
////					System.out.println("*********************aaaaaaaaaaaaaa*****"+timestamp);
////					System.out.println("*********************aaaaaaaaaaaaaa*****"+readwriteinterval + writetimestamp);
////					System.out.println("*********************aaaaaaaaaaaaaa*****"+countDownLatch.getCount());
//					cancel();
//				}
				lastallcount = allcount;
				allcount = 0;
				secklastallcount = seckallcount;
				seckallcount = 0;
				nosecklastallcount = noseckallcount;
				noseckallcount = 0;
				secklastallfailcount = seckallfailcount;
				seckallfailcount = 0;
				nosecklastallfailcount = noseckallfailcount;
				noseckallfailcount = 0;
				secklastallfailqueuecount = seckallfailqueuecount;
				seckallfailqueuecount = 0;
			}
		}, 5000, gathertime);

		try {
			if(cacheType == 0)
			{
				countDownLatch.await();
			}
			else if(cacheType == 1)
			{
				threadcountDownLatch.await();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (writeDataGeneratorType != 2 || readDataGeneratorType != 2)
			statisticalResults(System.nanoTime() - allTimeStart);
	}
	private void statisticalResults(long allTime) {
		long secktime = 0, seckcount = 0, seckallcount = 0, readcount = 0, time = 0, count = 0;
		long seckallfailcount = 0, seckallfailqueuecount = 0;
		float tps = 0, qps = 0;
		for (int i = 0; i < 10; i += 2) {
			time = 0;
			count = 0;
			for (int j = 0; j < threadNum; j++) {
				if (readresponseTimeList.get(j).length != 0) {
					time += readresponseTimeList.get(j)[i];
					count += readresponseTimeList.get(j)[i + 1];
					if (readresponseTimeList.get(j)[i + 1] != 0)
						qps += (float) readresponseTimeList.get(j)[i + 1] * 1000000.0 / readresponseTimeList.get(j)[i];
				}
			}

			if ((i / 2 + 1) == 1) {
				if (count != 0) {
					System.out.println("@@@@read time1**" + (float) time / count + "*****");
					logger.info("@@@@read time1**" + (float) time / count + "*****");
				}
			} else if ((i / 2 + 1) == 2) {
				if (count != 0) {
					System.out.println("@@@@read time2**" + (float) time / count + "*****");
					logger.info("@@@@read time2**" + (float) time / count + "*****");
				}
			} else if ((i / 2 + 1) == 3) {
				if (count != 0) {
					System.out.println("@@@@read time3**" + (float) time / count + "*****");
					logger.info("@@@@read time3**" + (float) time / count + "*****");
				}
			} else if ((i / 2 + 1) == 4) {
				if (count != 0) {
					System.out.println("@@@@read time4**" + (float) time / count + "*****");
					logger.info("@@@@read time4**" + (float) time / count + "*****");
				}
			} else if ((i / 2 + 1) == 5) {
				if (count != 0) {
					System.out.println("@@@@read time5**" + (float) time / count + "*****");
					logger.info("@@@@read time5**" + (float) time / count + "*****");
				}
			} else if ((i / 2 + 1) == 6) {
				if (count != 0) {
					System.out.println("@@@@read time6**" + (float) time / count + "*****");
					logger.info("@@@@read time6**" + (float) time / count + "*****");
				}
			}
			readcount += count;
		}
		// System.out.println("*******"+(allcount - lastallcount)+"*****");
		System.out.println("@@@@read Qps: " + qps);
		logger.info("@@@@read Qps: " + qps);
		for (int i = 0; i < 8; i += 2) {
			secktime = 0;
			seckcount = 0;
			for (int j = 0; j < threadNum; j++) {
				if (writeresponseTimeList.get(j).length != 0) {
					seckcount += writeresponseTimeList.get(j)[i];
					secktime += writeresponseTimeList.get(j)[i + 1];
					if (writeresponseTimeList.get(j)[i] != 0)
						tps += (float) writeresponseTimeList.get(j)[i] * 1000000.0
								/ writeresponseTimeList.get(j)[i + 1];
				}
			}
			if ((i / 2 + 1) == 1) {
				if (seckcount != 0) {
					System.out.println("@@@@tps1**" + (float) seckcount * threadNum * 1000000.0 / secktime + "*****");
					logger.info("@@@@tps1**" + (float) seckcount * 1000000.0 / secktime + "*****");
					System.out.println("@@@@time1(us)**" + (float) secktime / seckcount + "*****");
					logger.info("@@@@@time1**" + (float) secktime / seckcount + "*****");
				}
			} else if ((i / 2 + 1) == 2) {
				if (seckcount != 0) {
					System.out.println("@@@@tps2**" + (float) seckcount * threadNum * 1000000.0 / secktime + "*****");
					logger.info("@@@@tps2**" + (float) seckcount * 1000000.0 / secktime + "*****");
					System.out.println("@@@@time2(us)*" + (float) secktime / seckcount + "*****");
					logger.info("@@@@@time2**" + (float) secktime / seckcount + "*****");
				}
			} else if ((i / 2 + 1) == 3) {
				if (seckcount != 0) {
					System.out.println("@@@@tps3**" + (float) seckcount * threadNum * 1000000.0 / secktime + "*****");
					logger.info("@@@@tps3**" + (float) seckcount * 1000000.0 / secktime + "*****");
					System.out.println("@@@@@time3(us)*" + (float) secktime / seckcount + "*****");
					logger.info("@@@@time3**" + (float) secktime / seckcount + "*****");
				}
			} else if ((i / 2 + 1) == 4) {
				if (seckcount != 0) {
					System.out.println("@@@@tps4**" + (float) seckcount * threadNum * 1000000.0 / secktime + "*****");
					logger.info("@@@@tps4**" + (float) seckcount * 1000000.0 / secktime + "*****");
					System.out.println("@@@@time4(us)**" + (float) secktime / seckcount + "*****");
					logger.info("@@@@time4**" + (float) secktime / seckcount + "*****");
				}
			}
			seckallcount += seckcount;
		}
		for (int j = 0; j < threadNum; j++) {
			if (writeresponseTimeList.get(j).length != 0) {
				seckallfailcount += writeresponseTimeList.get(j)[16];
			}
		}
		for (int j = 0; j < threadNum; j++) {
			if (writeresponseTimeList.get(j).length != 0) {
				seckallfailqueuecount += writeresponseTimeList.get(j)[17];
			}
		}
		System.out.println("@@@@tps:" + tps + "*****");
		logger.info("@@@@tps:" + tps + "*****");
		System.out.println("@@@@alltime tps:" + (float) seckallcount * 1000000000 / allTime + "*****");
		logger.info("@@@@alltime tps:" + (float) seckallcount * 1000000000 / allTime + "*****");
		System.out.println("@@@@succeed:" + seckallcount + "*****");
		logger.info("@@@@tps:" + seckallcount + "*****");
		System.out.println("@@@@queuefail:" + seckallfailqueuecount + "*****");
		logger.info("@@@@queuefail:" + seckallfailqueuecount + "*****");
		System.out.println("@@@@fail:" + seckallfailcount + "*****");
		logger.info("@@@@fail:" + seckallfailcount + "*****");

	}

	public void addOperation(Operation<MessageProto.Message> operation) {
		try {
			if (operation.type == 2) {
				if(operation.message.getMessageWrite().getSeckorder() == 1){
					if(cacheType == 0){
						sckoperationQueues.get((int)(Math.random() * sckoperationQueues.size())).put(operation);
					}
					else if(cacheType == 1){
						if(hashflag){
							sckoperationQueues.get((operation.message.getMessageWrite().getSlskpID()/baseNum) % threadNum).put(operation);
						}
						else{
							sckoperationQueues.get(getIndex(operation.message.getMessageWrite().getSlskpID())).put(operation);
						}
							
					}
				}
				else{
					sckoperationQueues.get((int)(Math.random() * sckoperationQueues.size())).put(operation);
				}
			} else if (operation.type == 3) {
				sckotheroperationQueues.get((int)(sckotheroperationQueues.size() * Math.random())).put(operation);
			} else if (operation.type == 1) {
//				System.out.println("!!!!!!!!!!!!!!" + a + "*****");
				sckreadoperationQueues.get((int)(Math.random() * sckreadoperationQueues.size())).put(operation);
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void createHashmap()
	{
		SeckillItem item = new SeckillItem();
		if(dbData.size()>=threadNum)
		{
			hashflag = true;
			for(int i = 0;i < dbData.size();i++)
			{
				item = dbData.get(i);
				dbDataList.get((item.skpkey/baseNum)%threadNum).put(item.skpkey, item.plancount);
			}
		}
		else
		{
			hashflag = false;
			Collections.sort(dbData);
			int divi = threadNum / dbData.size();
			int residue = threadNum % dbData.size();
			int num = 0;
			for(int i= 0; i < residue; i++)
			{
				ArrayList<Integer> arr = new ArrayList<Integer>();
				arr.add(num);
				item = dbData.get(i);
				dbDataList.get(num++).put(item.skpkey, item.plancount);
				hashselect.put(item.skpkey, arr);
			}
			for(int i = residue; i < dbData.size(); i++)
			{
				ArrayList<Integer> arr = new ArrayList<Integer>();
				item = dbData.get(i);
				for(int j = 0; j < divi; j++)
				{
					arr.add(num);
					if(j==0)
						dbDataList.get(num++).put(item.skpkey, (item.plancount/divi) +(item.plancount%divi));
					else
						dbDataList.get(num++).put(item.skpkey, item.plancount/divi);
				}
				hashselect.put(item.skpkey, arr);
			}
		}
		
	}
	
	public int getIndex(int id)
	{
		ArrayList<Integer> arr = hashselect.get(id);
		return arr.get((int) (arr.size()* Math.random()));
	}
	

}

class MastercontrolThread implements Runnable {
	private ArrayBlockingQueue<Operation<MessageProto.Message>> sckoperationQueues = null;
	private ArrayBlockingQueue<Operation<MessageProto.Message>> sckotheroperationQueues = null;
	private ArrayBlockingQueue<Operation<MessageProto.Message>> sckreadoperationQueues = null;
	private ArrayBlockingQueue<DBOperatorThread> waitQueues = null;
	private CountDownLatch countDownLatch;
	private int writeDataGeneratorType, conflictWorkloadcount;
	private int readDataGeneratorType, readWorkloadcount;
	private int confilictCount = 0, tallcount = 0;
	private Random random = new Random();

	public MastercontrolThread(ArrayBlockingQueue<Operation<MessageProto.Message>> sckreadoperationQueues,
			ArrayBlockingQueue<Operation<MessageProto.Message>> sckoperationQueues,
			ArrayBlockingQueue<Operation<MessageProto.Message>> sckotheroperationQueues,
			ArrayBlockingQueue<DBOperatorThread> waitQueue, int writeDataGeneratorType, int conflictWorkloadcount,
			int readDataGeneratorType, int readWorkloadcount, CountDownLatch countDownLatch) {
		this.sckreadoperationQueues = sckreadoperationQueues;
		this.sckoperationQueues = sckoperationQueues;
		this.sckotheroperationQueues = sckotheroperationQueues;
		this.waitQueues = waitQueue;
		this.writeDataGeneratorType = writeDataGeneratorType;
		this.conflictWorkloadcount = conflictWorkloadcount;
		this.readDataGeneratorType = readDataGeneratorType;
		this.readWorkloadcount = readWorkloadcount;
		this.countDownLatch = countDownLatch;
	}

	public void run() {
		if (writeDataGeneratorType == 1 || writeDataGeneratorType == 3 || writeDataGeneratorType == 4
				|| readDataGeneratorType == 1|| readDataGeneratorType == 3) {
			System.out.println("@@@@MastercontrolThread start");
			System.out.println("@@@@" + conflictWorkloadcount);
			while(sckoperationQueues.size() == 0&& sckreadoperationQueues.size() == 0)
			{
				for(int i=0;i<10000;i++);
			}
			for (int i = 0; i < (conflictWorkloadcount * 2 + readWorkloadcount); i++) {
				if (i > (int) ((conflictWorkloadcount + readWorkloadcount) / 10) && sckoperationQueues.size() == 0
						&& sckotheroperationQueues.size() == 0&& sckreadoperationQueues.size() == 0)
					break;
//				System.out.println("@@@@" + i);
				if(handle()==0)
					i-=1;
			}
			System.out.println("@@@@confilictCount:" + confilictCount + "*****" + tallcount);
			countDownLatch.countDown();
		} else if (writeDataGeneratorType == 2 || readDataGeneratorType == 2) {
//			int k = 0;
//			int count = 0;
			while (true) {
				handle();
			}
		}
	}

	private int handle() {
		DBOperatorThread obo = null;
		Operation<MessageProto.Message> operation = null;
		try {
			if((sckotheroperationQueues.size() == 0&&sckreadoperationQueues.size() == 0) || random.nextFloat() < 0.35)
			{
				if(sckoperationQueues.size()>0)
				{
					operation = sckoperationQueues.take();
				}
				else{
					if(sckreadoperationQueues.size() == 0||random.nextFloat() < 0.5){
						if(sckotheroperationQueues.size()>0){
							operation = sckotheroperationQueues.take();
						}
						else if(sckreadoperationQueues.size() == 0){
							return 0;
						}
						else{
							operation = sckreadoperationQueues.take();
						}
					}
					else
					{
						if(sckreadoperationQueues.size()>0){
							operation = sckreadoperationQueues.take();
						}
						else{
							return 0;
						}
					}
				}
			}
			else if((sckoperationQueues.size() == 0&&sckreadoperationQueues.size() == 0) || random.nextFloat() < 0.65)
			{
				if(sckotheroperationQueues.size()>0)
					operation = sckotheroperationQueues.take();
				else{
					if(sckoperationQueues.size() == 0 || random.nextFloat() < 0.5){
						if(sckreadoperationQueues.size()>0){
							operation = sckreadoperationQueues.take();
						}
						else if(sckoperationQueues.size()==0){
							return 0;
						}
						else{
							operation = sckoperationQueues.take();
						}
					}
					else{
						if(sckoperationQueues.size()>0){
							operation = sckoperationQueues.take();
						}
						else{
							return 0;
						}
					}
				}
			}
			else if((sckoperationQueues.size() == 0&&sckotheroperationQueues.size() == 0) || random.nextFloat() < 1.0)
			{
				if(sckreadoperationQueues.size()>0){
					operation = sckreadoperationQueues.take();
//					System.out.println("~~~~~~*******333333333333");
				}
				else
				{
					if(sckoperationQueues.size() == 0 || random.nextFloat() < 0.5){
						if(sckotheroperationQueues.size()>0){
							operation = sckotheroperationQueues.take();
						}
						else if(sckoperationQueues.size()==0){
							return 0;
						}
						else{
							operation = sckoperationQueues.take();
						}
					}
					else
					{
						if(sckoperationQueues.size()>0){
							operation = sckoperationQueues.take();
						}
						else{
							return 0;
						}
					}
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(operation == null)
			return 0;
		int CN = 0;
		if (operation.type == 1 || operation.type == 3) {
			try {
				obo = waitQueues.take();
				obo.notifythis(operation.message, operation.type, operation.time);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} else {
			CN = operation.N;
			while (true) {
				if (waitQueues.size() >= CN)
					break;
			}
			try {
				if (CN > 1)
					confilictCount++;
				for (int i = 0; i < CN; i++) {
					obo = waitQueues.take();
					obo.notifythis(operation.message.getMessageWrite(), i, 2, operation.time);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return 1;
	}
}

class DBOperatorThread extends Thread // implements Runnable
{
	private ArrayBlockingQueue<DBOperatorThread> waitQueue = null;
	private Connection conn = null;
	private PreparedStatement rpstmt1 = null, rpstmt2 = null, rpstmt3 = null, rpstmt4 = null, rpstmt5 = null,
			rpstmt6 = null;
	private PreparedStatement wpstmt1 = null, wpstmt2 = null, wpstmt3 = null, wpstmt4 = null, wpstmt5 = null,
			wpstmt6 = null, wpstmt7 = null, wpstmt9 = null, wpstmt10 = null, wpstmt11 = null, wpstmt12 = null,
			wpstmt12_ = null, wpstmt13 = null, wpstmt14 = null, wpstmt15 = null, wpstmt16 = null, wpstmt17 = null,
			wpstmt18 = null, wpstmt19 = null, wpstmt20 = null;
	CallableStatement rcallpstmt1 = null, rcallpstmt2 = null, rcallpstmt3 = null, rcallpstmt4 = null,
			rcallpstmt5 = null, rcallpstmt6 = null, wcallpstmt1 = null, wcallpstmt_submit = null, 
			wcallpstmt_pay_one = null, wcallpstmt_pay_two = null, wcallpstmt_cancel = null,
			wcallpstmt9 = null, wcallpstmt10 = null, wcallpstmt17 = null, wcallpstmt18 = null, wcallpstmt19 = null;
	private Statement pst = null;
	private CopyOnWriteArrayList<long[]> readresponseTimeList = null;
	private CopyOnWriteArrayList<long[]> writeresponseTimeList = null;
	private CopyOnWriteArrayList<long[]> readclientresponseTimeList = null;
	private CopyOnWriteArrayList<long[]> writeclientresponseTimeList = null;
	public int thread_th = 0;
	public OtherOperator otherOperator = null;
	public int timestampcount = 0;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	private volatile boolean isPark = false;
	private int testPayimpact, testPaysck;
	private int writeDataGeneratorType, DBType;
	volatile long[] readresponseTime = new long[12];
	volatile long[] readclientresponseTime = new long[12];
	Random random = new Random();
	float paycancelratio;
	volatile long[] writeresponseTime = new long[20];
	volatile long[] writeclientresponseTime = new long[20];
	SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	MessageProto.MessageWrite sckmessage = null;
	MessageProto.Message RWMessage = null;
	int sckorder_i = 0;
	int messageType = 0, cacheType = 0, hashMapType = 0;
	private HashMap<Integer, Integer> data = null;
	long clienttime = 0;
	CountDownLatch threadcountDownLatch = null;
	private volatile static AtomicInteger msgID = new AtomicInteger();
	ArrayBlockingQueue<Operation<MessageProto.Message>> sckreadoperationQueues = null;
	ArrayBlockingQueue<Operation<MessageProto.Message>> sckoperationQueues = null;
	ArrayBlockingQueue<Operation<MessageProto.Message>> sckotheroperationQueues = null;
	// private static Logger logger = Logger.getLogger("infoLogger");

	public DBOperatorThread(ArrayBlockingQueue<DBOperatorThread> waitQueue,
			CopyOnWriteArrayList<long[]> readresponseTimeList, CopyOnWriteArrayList<long[]> readclientresponseTimeList,
			CopyOnWriteArrayList<long[]> writeresponseTimeList,
			CopyOnWriteArrayList<long[]> writeclientresponseTimeList, Connection conn, int thread_th,
			OtherOperator otherOperator, int timestampcount, float payRatio, float cancelRatio, 
			float queryRatio, int testPayimpact, int testPaysck, int writeDataGeneratorType, 
			int DBType, int cacheType, int hashMapType) {
		super();
		this.waitQueue = waitQueue;
		this.readresponseTimeList = readresponseTimeList;
		this.writeresponseTimeList = writeresponseTimeList;
		this.readclientresponseTimeList = readclientresponseTimeList;
		this.writeclientresponseTimeList = writeclientresponseTimeList;
		this.conn = conn;
		this.thread_th = thread_th;
		this.otherOperator = otherOperator;
		this.timestampcount = timestampcount;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		this.testPayimpact = testPayimpact;
		this.testPaysck = testPaysck;
		this.writeDataGeneratorType = writeDataGeneratorType;
		this.DBType = DBType;
		this.cacheType = cacheType;
		this.hashMapType = hashMapType;
		try {
			init();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public DBOperatorThread(ArrayBlockingQueue<DBOperatorThread> waitQueue,
			CopyOnWriteArrayList<long[]> readresponseTimeList, CopyOnWriteArrayList<long[]> readclientresponseTimeList,
			CopyOnWriteArrayList<long[]> writeresponseTimeList,
			CopyOnWriteArrayList<long[]> writeclientresponseTimeList, Connection conn, int thread_th,
			OtherOperator otherOperator, int timestampcount, float payRatio, float cancelRatio, float queryRatio,
			int testPayimpact, int testPaysck, int writeDataGeneratorType, int DBType, int cacheType, 
			CountDownLatch threadcountDownLatch, int hashMapType, HashMap<Integer, Integer> data,
			ArrayBlockingQueue<Operation<MessageProto.Message>> sckreadoperationQueues,
			ArrayBlockingQueue<Operation<MessageProto.Message>> sckoperationQueues,
			ArrayBlockingQueue<Operation<MessageProto.Message>> sckotheroperationQueues ) {
		super();
		this.waitQueue = waitQueue;
		this.readresponseTimeList = readresponseTimeList;
		this.writeresponseTimeList = writeresponseTimeList;
		this.readclientresponseTimeList = readclientresponseTimeList;
		this.writeclientresponseTimeList = writeclientresponseTimeList;
		this.conn = conn;
		this.thread_th = thread_th;
		this.otherOperator = otherOperator;
		this.timestampcount = timestampcount;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		this.testPayimpact = testPayimpact;
		this.testPaysck = testPaysck;
		this.writeDataGeneratorType = writeDataGeneratorType;
		this.DBType = DBType;
		this.cacheType = cacheType;
		this.threadcountDownLatch = threadcountDownLatch;
		this.hashMapType = hashMapType;
		this.data = data;
		this.sckreadoperationQueues = sckreadoperationQueues;
		this.sckoperationQueues = sckoperationQueues;
		this.sckotheroperationQueues = sckotheroperationQueues;
		try {
			init();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void init() throws SQLException {
		pst = conn.createStatement();
		if(DBType==3){
			pst.execute("set @@session.ob_query_timeout=9000000000;");
		}
		if(DBType==4)
		{
			rcallpstmt1 = conn.prepareCall("{call rpstmt1 (?)}");
			rcallpstmt2 = conn.prepareCall("{call rpstmt2 (?,?)}");
			rcallpstmt3 = conn.prepareCall("{call rpstmt3 (?,?,?)}");
			rcallpstmt4 = conn.prepareCall("{call rpstmt4 (?,?,?,?)}");
			rcallpstmt5 = conn.prepareCall("{call rpstmt5 (?,?)}");
			rcallpstmt6 = conn.prepareCall("{call rpstmt6 (?)}");
			//write:
			wcallpstmt1 = conn.prepareCall("{call wpstmt1 (?)}");
			wcallpstmt_submit = conn.prepareCall("{call Submit_Order (?,?,?,?,?,?)}");
			wcallpstmt9 = conn.prepareCall("{call wpstmt9 (?)}");
			wcallpstmt10 = conn.prepareCall("{call wpstmt10 (?)}");
			wcallpstmt_pay_two = conn.prepareCall("{call Pay_Order_two (?,?,?)}");
			wcallpstmt_pay_one = conn.prepareCall("{call Pay_Order_one (?,?,?)}");
			wcallpstmt_cancel = conn.prepareCall("{call Cancel_Order (?,?)}");
			wcallpstmt17 = conn.prepareCall("{call wpstmt17 (?)}");
			wcallpstmt18 = conn.prepareCall("{call wpstmt18 (?)}");
			wcallpstmt19 = conn.prepareCall("{call wpstmt19 (?)}"); 
		}
		else
		{
			rpstmt1 = conn.prepareStatement("select * from item where i_itemkey = ?");
			rpstmt2 = conn.prepareStatement("select * from item where i_type = ? limit ?");
			rpstmt3 = conn.prepareStatement("select * from item where i_type = ? and i_name like ? limit ?");
			rpstmt4 = conn.prepareStatement("select * from item where i_type = ? and i_price between ? and ? limit ?");
			rpstmt5 = conn.prepareStatement("select * from item where i_suppkey = ? limit ?");
			rpstmt6 = conn.prepareStatement("select * from seckillplan where sl_skpkey = ? ");
			// submit order seckill
			wpstmt1 = conn.prepareStatement("select sl_price from seckillplan where sl_skpkey = ?");
			wpstmt2 = conn.prepareStatement(
					"update seckillplan set sl_skpcount = sl_skpcount + 1 where sl_skpkey = ? and sl_skpcount < sl_plancount");
			wpstmt3 = conn.prepareStatement(
					"insert into orders (o_orderkey, o_custkey, o_skpkey, o_price, o_orderdate, o_state) values (?, ?, ?, ?, ?, 0)");
			wpstmt4 = conn.prepareStatement(
					"insert into orderitem (oi_orderkey, oi_itemkey, oi_count, oi_price) values (?, ?, 1, ?)");
			// submit order noseckill
			wpstmt5 = conn.prepareStatement("select i_price from item where i_itemkey = ?");
			wpstmt6 = conn.prepareStatement("update item set i_count = i_count - 1 where i_itemkey = ? and i_count >= 1");
			wpstmt7 = conn.prepareStatement(
					"insert into orders (o_orderkey, o_custkey, o_price, o_orderdate, o_state) values (?, ?, ?, ?,0)");
			// p4
			// pay order seckill
			wpstmt9 = conn.prepareStatement("select * from orders where o_custkey = ? and o_state = 0");
			wpstmt10 = conn.prepareStatement("select o_price from orders where o_orderkey = ?");
			wpstmt11 = conn.prepareStatement("update orders set o_state = 1,o_paydate = ? where o_orderkey = ?");
			wpstmt12 = conn.prepareStatement("update seckillpay set sa_paycount = sa_paycount + 1 where sa_skpkey = ?");
			wpstmt12_ = conn.prepareStatement("update seckillplan set sl_paycount = sl_paycount + 1 where sl_skpkey = ?");
			// pay order noseckill
			// p9\p10\p11
			// cancel order seckill
			// p9
			wpstmt13 = conn.prepareStatement("update orders set o_state = 2 where o_orderkey = ? ");
			wpstmt14 = conn.prepareStatement("update seckillplan set sl_skpcount = sl_skpcount - 1 where sl_skpkey = ?");
			// cancel order noseckill
			// p9\p13
			wpstmt15 = conn.prepareStatement("select oi_itemkey, oi_count from orderitem where oi_orderkey = ?");
			wpstmt16 = conn.prepareStatement("update item set i_count = i_count + ? where i_itemkey = ?");
			// select order
			wpstmt17 = conn.prepareStatement("select * from orders where o_custkey = ? limit " + 10);
			wpstmt18 = conn.prepareStatement("select * from orders where o_orderkey = ?");
			wpstmt19 = conn.prepareStatement("select * from orderitem where oi_orderkey = ?");
		}
	}

	public void run() {
		if(cacheType == 0){
			while (true) {
				try {
					isPark = false;
					waitQueue.put(this);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				while (true) {
					if (isPark) {
						break;
					}
				}
				if (messageType == 1) {
					ReadOperator(RWMessage.getMessageRead(), clienttime);
				} else if (messageType == 3) {
					WriteOperator(RWMessage.getMessageWrite(), 0, clienttime);
				} else if (messageType == 2) {
					WriteOperator(sckmessage, sckorder_i, clienttime);
				}
			}
		}
		else if(cacheType == 1)
		{
			while(sckoperationQueues.size() == 0&& sckreadoperationQueues.size() == 0)
			{
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
//				System.out.println("!!!!!!!!!!!!!!!!!!"+sckoperationQueues.size()+"!!!!!!!!!!!!!!!!!!"+sckreadoperationQueues.size());
			}
			int count = 0;
			while (true)  {
//				System.out.println("***************"+count);
				if (sckoperationQueues.size() == 0&& sckotheroperationQueues.size() == 0&& sckreadoperationQueues.size() == 0)
				{
					count ++ ;
					if(count > 10000)
					{
						System.out.println("~~~~~~~~~~~~~~~"+count);
						break;
					}
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					continue;
				}
//				System.out.println("************"+sckoperationQueues.size());
//				Operation<MessageProto.Message> operation = DBOperator.getOperation(sckoperationQueues, 
//						sckreadoperationQueues, sckotheroperationQueues);
				Operation<MessageProto.Message> operation = null;
				try {
					if((sckotheroperationQueues.size() == 0&&sckreadoperationQueues.size() == 0) || random.nextFloat() < 0.35)
					{
						if(sckoperationQueues.size()>0)
						{
							operation = sckoperationQueues.take();
						}
						else{
							if(sckreadoperationQueues.size() == 0||random.nextFloat() < 0.5){
								if(sckotheroperationQueues.size()>0){
									operation = sckotheroperationQueues.take();
								}
								else if(sckreadoperationQueues.size() == 0){
									operation = null;
								}
								else{
									operation = sckreadoperationQueues.take();
								}
							}
							else
							{
								if(sckreadoperationQueues.size()>0){
									operation = sckreadoperationQueues.take();
								}
								else{
									operation = null;
								}
							}
						}
					}
					else if((sckoperationQueues.size() == 0&&sckreadoperationQueues.size() == 0) || random.nextFloat() < 0.65)
					{
						if(sckotheroperationQueues.size()>0)
							operation = sckotheroperationQueues.take();
						else{
							if(sckoperationQueues.size() == 0 || random.nextFloat() < 0.5){
								if(sckreadoperationQueues.size()>0){
									operation = sckreadoperationQueues.take();
								}
								else if(sckoperationQueues.size()==0){
									operation = null;
								}
								else{
									operation = sckoperationQueues.take();
								}
							}
							else{
								if(sckoperationQueues.size()>0){
									operation = sckoperationQueues.take();
								}
								else{
									operation = null;
								}
							}
						}
					}
					else if((sckoperationQueues.size() == 0&&sckotheroperationQueues.size() == 0) || random.nextFloat() < 1.0)
					{
						if(sckreadoperationQueues.size()>0){
							operation = sckreadoperationQueues.take();
//							System.out.println("~~~~~~*******333333333333");
						}
						else
						{
							if(sckoperationQueues.size() == 0 || random.nextFloat() < 0.5){
								if(sckotheroperationQueues.size()>0){
									operation = sckotheroperationQueues.take();
								}
								else if(sckoperationQueues.size()==0){
									operation = null;
								}
								else{
									operation = sckoperationQueues.take();
								}
							}
							else
							{
								if(sckoperationQueues.size()>0){
									operation = sckoperationQueues.take();
								}
								else{
									operation = null;
								}
							}
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if(operation != null)
				{
					if (operation.type == 1) {
						ReadOperator(operation.message.getMessageRead(), operation.time);
					} else if (operation.type == 2 || operation.type == 3) {
						WriteOperator(operation.message.getMessageWrite(), 0, operation.time);
					}	
				}
			}
			threadcountDownLatch.countDown();
		}
	}

	public void notifythis(MessageProto.MessageWrite message, int order_i, int type, long time) {
		sckmessage = message;
		sckorder_i = order_i;
		messageType = type;
		clienttime = time;
		isPark = true;
	}

	public void notifythis(MessageProto.Message message, int type, long time) {
		RWMessage = message;
		messageType = type;
		clienttime = time;
		isPark = true;
	}

	public void ReadOperator(MessageProto.MessageRead message, long clienttime) {
		try {
			if (message.getType() == 1) {
				long start = 0;
				if(DBType == 4){
					rcallpstmt1.setInt(1, message.getItemID());
					start = System.nanoTime();
					rcallpstmt1.executeQuery();
				}
				else{
					rpstmt1.setInt(1, message.getItemID());
					start = System.nanoTime();
					rpstmt1.executeQuery();
				}
				readresponseTime[0] += (System.nanoTime() - start) / 1000;
				readresponseTime[1]++;
				readclientresponseTime[0] += (System.nanoTime() - clienttime)/1000;
				readclientresponseTime[1]++;
			} else if (message.getType() == 2) {
				long start = 0;
				if(DBType == 4){
					rcallpstmt2.setInt(1, message.getTypeID());
					rcallpstmt2.setInt(2, message.getLimite());
					start = System.nanoTime();
					rcallpstmt2.executeQuery();
				}
				else{
					rpstmt2.setInt(1, message.getTypeID());
					rpstmt2.setInt(2, message.getLimite());
					start = System.nanoTime();
					rpstmt2.executeQuery();
				}
				readresponseTime[2] += (System.nanoTime() - start) / 1000;
				readresponseTime[3]++;
				readclientresponseTime[2] += (System.nanoTime() - clienttime)/1000;
				readclientresponseTime[3]++;
			} else if (message.getType() == 3) {
				long start = 0;
				if(DBType == 4){
					rcallpstmt3.setInt(1, message.getTypeID());
					rcallpstmt3.setString(2, RanDataGene.getChar() + RanDataGene.getChar() + "%");
					rcallpstmt3.setInt(3, message.getLimite());
					start = System.nanoTime();
					rcallpstmt3.executeQuery();
				}
				else{
					rpstmt3.setInt(1, message.getTypeID());
					rpstmt3.setString(2, RanDataGene.getChar() + RanDataGene.getChar() + "%");
					rpstmt3.setInt(3, message.getLimite());
					start = System.nanoTime();
					rpstmt3.executeQuery();
				}
				readresponseTime[4] += (System.nanoTime() - start) / 1000;
				readresponseTime[5]++;
				readclientresponseTime[4] += (System.nanoTime() - clienttime)/1000;
				readclientresponseTime[5]++;
			} else if (message.getType() == 4) {
				float price1 = RanDataGene.getInteger(1, 500) - random.nextFloat();
				float price2 = price1 + RanDataGene.getInteger(1, 500);
				long start = 0;
				if(DBType == 4){
					rcallpstmt4.setInt(1, message.getTypeID());
					rcallpstmt4.setFloat(2, price1);
					rcallpstmt4.setFloat(3, price2);
					rcallpstmt4.setInt(4, message.getLimite());
					start = System.nanoTime();
					rcallpstmt4.executeQuery();
				}
				else{
					rpstmt4.setInt(1, message.getTypeID());
					rpstmt4.setFloat(2, price1);
					rpstmt4.setFloat(3, price2);
					rpstmt4.setInt(4, message.getLimite());
					start = System.nanoTime();
					rpstmt4.executeQuery();
				}
				readresponseTime[6] += (System.nanoTime() - start) / 1000;
				readresponseTime[7]++;
				readclientresponseTime[6] += (System.nanoTime() - clienttime)/1000;
				readclientresponseTime[7]++;
			} else if (message.getType() == 5) {
				long start = 0;
				if(DBType == 4){
					rcallpstmt5.setInt(1, message.getSuppID());
					rcallpstmt5.setInt(2, message.getLimite());
					start = System.nanoTime();
					rcallpstmt5.executeQuery();
				}
				else{
					rpstmt5.setInt(1, message.getSuppID());
					rpstmt5.setInt(2, message.getLimite());
					start = System.nanoTime();
					rpstmt5.executeQuery();
				}
				readresponseTime[8] += (System.nanoTime() - start) / 1000;
				readresponseTime[9]++;
				readclientresponseTime[8] += (System.nanoTime() - clienttime)/1000;
				readclientresponseTime[9]++;
			} else if (message.getType() == 6) {
				long start = 0;
				if(DBType == 4){
					rcallpstmt6.setInt(1, message.getSlskpID());
					start = System.nanoTime();
					rcallpstmt6.executeQuery();
				}
				else{
					rpstmt6.setInt(1, message.getSlskpID());
					start = System.nanoTime();
					rpstmt6.executeQuery();
				}
				readresponseTime[10] += (System.nanoTime() - start) / 1000;
				readresponseTime[11]++;
//				System.out.println("~~~~~~*******66666666666666~~~~~~"+(System.nanoTime() - start));
//				System.out.println("~~~~~~*******66666666666666~~~~~~"+(System.nanoTime()-message.getTimestamp()));
				readclientresponseTime[10] +=(System.nanoTime() - clienttime)/1000;
				readclientresponseTime[11]++;
			}
			readresponseTimeList.set(thread_th, readresponseTime);
			readclientresponseTimeList.set(thread_th, readclientresponseTime);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void WriteOperator(MessageProto.MessageWrite message, int order_i, long clienttime) {
		try {
			if (message.getSeckorder() == 1) {
				if (message.getType() == 1) {
					// seck sbmit
					int count = -1;
					boolean hash = true;
					if(hashMapType == 1)
					{
						count = data.get(message.getSlskpID());
						if(count<=0){
							writeresponseTime [17] ++;
//							System.out.println("~~~~~~*******~~~~~~"+writeresponseTime [17]);
							hash = false;
						}
					}
					// 测试直接加压与DB
					// int count = 1;
					// System.out.println("!!!!!!!!!!!"+count);
					if(hash){
						int orderkey = message.getOrderID() + order_i;
						// logger.info("!!!!!!!!!!!!!!"+ orderkey);
						// System.out.println("~~~~~~~~~~"+message.getMessageWrite().getOrderID());
						try {
							long transStart = 0;
							if(DBType==4)
							{
								wcallpstmt1.setInt(1, message.getSlskpID());
//								ResultSet rs = wcallpstmt1.executeQuery();
								Double price = 0.0;
//								if (rs.next()) {
//									price = rs.getDouble(1);
//								} else {
//									System.out.println("select error in SubmitOrderThread!");
//									return;
//								}
								transStart = System.nanoTime();
								wcallpstmt_submit.setInt(1, message.getSlskpID());
								wcallpstmt_submit.setInt(2, orderkey);
								wcallpstmt_submit.setInt(3, message.getCostomID());
								wcallpstmt_submit.setInt(4, Integer.valueOf(message.getItemID()));
								wcallpstmt_submit.setDouble(5, price);
								wcallpstmt_submit.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
//								int rows = wcallpstmt_submit.executeUpdate();
//								if (rows == 0) {
//									writeresponseTime[16]++;
//									writeclientresponseTime[16]++;
//									return;
//								}
							}
							else
							{
								wpstmt1.setInt(1, message.getSlskpID());
//								ResultSet rs = wpstmt1.executeQuery();
								Double price = 0.0;
//								if (rs.next()) {
//									price = rs.getDouble(1);
//								} else {
//									System.out.println("select error in SubmitOrderThread!");
//									return;
//								}
								conn.setAutoCommit(false);
								transStart = System.nanoTime();
								wpstmt2.setInt(1, message.getSlskpID());
//								int rows = wpstmt2.executeUpdate();
//								if (rows == 0) {
//									conn.commit();
//									writeresponseTime[16]++;
//									writeclientresponseTime[16]++;
//									return;
//								}
								wpstmt3.setInt(1, orderkey);
								wpstmt3.setInt(2, message.getCostomID());
								wpstmt3.setInt(3, message.getSlskpID());
								wpstmt3.setDouble(4, price);
								if(DBType==1||DBType==3){//mysql;cedar
									wpstmt3.setString(5, dateFormater.format(new Date(new java.util.Date().getTime())));
								}
								if(DBType==2){// postgres
									wpstmt3.setDate(5, new Date(new java.util.Date().getTime()));
								}
//								wpstmt3.executeUpdate();
								wpstmt4.setInt(1, orderkey);
								wpstmt4.setInt(2, Integer.valueOf(message.getItemID()));
								wpstmt4.setDouble(3, price);
//								wpstmt4.executeUpdate();
//								conn.commit();
							}
							//
							sleep(3);
							writeresponseTime[1] += (System.nanoTime() - transStart) / 1000;
							writeresponseTime[0]++;
							writeclientresponseTime[1] += (System.nanoTime() - clienttime)/1000;
							writeclientresponseTime[0]++;
							// 记录data的
							if(hashMapType ==1 ){
								data.put(message.getSlskpID(),count-1);
							}
							if (random.nextFloat() < queryRatio) {
								otherTask othertask = new otherTask(1, orderkey, message.getCostomID(),
										message.getSlskpID(), 3);
								if (writeDataGeneratorType == 2) {
									otherOperator.addotherOperator(othertask);
								} else{
									otherOperator.addotherOperator(othertask, ((int) (timestampcount * Math.random())));
								}
							}
							paycancelratio = random.nextFloat();
							if (paycancelratio < payRatio) {
								if (testPayimpact == 1) {
									MessageProto.Message.Builder Message = MessageProto.Message.newBuilder();
									Message.setMsgID(getMsgID());
									Message.setType(2);
									MessageProto.MessageWrite.Builder workmessage = MessageProto.MessageWrite.newBuilder();
									workmessage.setType(2);
	//								workmessage.setTimestamp(System.nanoTime());
									workmessage.setOrderID(orderkey);
									workmessage.setCostomID(message.getCostomID());
									workmessage.setSlskpID(message.getSlskpID());
									workmessage.setSeckorder(1);
									Message.setMessageWrite(workmessage.build());
									Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(
											Message.build(), 1, 3, System.nanoTime());
									ProceseNode.getDbOperator().addOperation(operation);
								} else {
									otherTask othertask = new otherTask(1, orderkey, message.getCostomID(),
											message.getSlskpID(), 2);
									if (writeDataGeneratorType == 2) {
										otherOperator.addotherOperator(othertask);
									} else {
										otherOperator.addotherOperator(othertask, ((int) (timestampcount * Math.random())));
									}
								}
	
							} else if (paycancelratio < (payRatio + cancelRatio)) {
								otherTask othertask = new otherTask(1, orderkey, message.getCostomID(),
										message.getSlskpID(), 4);
								if (writeDataGeneratorType == 2) {
									otherOperator.addotherOperator(othertask);
								} else{
								otherOperator.addotherOperator(othertask, ((int) (timestampcount * Math.random())));
								}
							}
						} catch (Exception e) {
							if(DBType!=4)
							{conn.rollback();}
							writeresponseTime[16]++;
							writeclientresponseTime[16]++;
							e.printStackTrace();
						} finally {
							if(DBType!=4)
							{conn.setAutoCommit(true);}
						}
					}
				} else if (message.getType() == 2) {
					int orderkey = message.getOrderID() + order_i;
					// seck pay
					try {
						long transStart = 0;
						if(DBType==4)
						{
//							wcallpstmt9.setInt(1, message.getCostomID());
//							wcallpstmt9.executeQuery();
							wcallpstmt10.setInt(1, orderkey);
//							wcallpstmt10.executeQuery();
							transStart = System.nanoTime();
							if (testPaysck == 1) {
								wcallpstmt_pay_one.setInt(1, message.getSlskpID());
								wcallpstmt_pay_one.setInt(2, orderkey);
								wcallpstmt_pay_one.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
								wcallpstmt_pay_one.executeUpdate();
							} else {
								wcallpstmt_pay_two.setInt(1, message.getSlskpID());
								wcallpstmt_pay_two.setInt(2, orderkey);
								wcallpstmt_pay_two.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
//								wcallpstmt_pay_two.executeUpdate();
							}
						}
						else
						{
							wpstmt9.setInt(1, message.getCostomID());
//							wpstmt9.executeQuery();
							wpstmt10.setInt(1, orderkey);
//							wpstmt10.executeQuery();
							conn.setAutoCommit(false);
							transStart = System.nanoTime();
							if(DBType==1||DBType==3){//mysql;cedar
								wpstmt11.setString(1, dateFormater.format(new Date(new java.util.Date().getTime())));
							}
							if(DBType==2){// post
								wpstmt11.setDate(1, new Date(new java.util.Date().getTime()));
							}
							// System.out.println("##############pay!!!!!!!!!!!"+orderkey);
							wpstmt11.setInt(2, orderkey);
//							wpstmt11.executeUpdate();
							if (testPaysck == 1) {
								wpstmt12_.setInt(1, message.getSlskpID());
								wpstmt12_.executeUpdate();
							} else {
								wpstmt12.setInt(1, message.getSlskpID());
//								wpstmt12.executeUpdate();
							}
//							conn.commit();
						}
						sleep(1);
						writeresponseTime[3] += (System.nanoTime() - transStart) / 1000;
						writeresponseTime[2]++;
						writeclientresponseTime[3] += (System.nanoTime() - clienttime)/1000;
						writeclientresponseTime[2]++;
					} catch (Exception e) {
						if(DBType!=4)
						{conn.rollback();}
						writeresponseTime[16]++;
						writeclientresponseTime[16]++;
						e.printStackTrace();
					} finally {
						if(DBType!=4)
						{conn.setAutoCommit(true);}
					}
				} else if (message.getType() == 4) {
					int orderkey = message.getOrderID() + order_i;
					// logger.info("3333!!!!!!!!!!!!!!"+ orderkey);
					// seck cancel
					try {
						long transStart = 0;
						if(DBType==4)
						{
							wcallpstmt9.setInt(1, message.getCostomID());
//							wcallpstmt9.executeQuery();
							transStart = System.nanoTime();
							wcallpstmt_cancel.setInt(1, message.getSlskpID());
							wcallpstmt_cancel.setInt(2, orderkey);
//							wcallpstmt_cancel.executeUpdate();
						}
						else
						{
							wpstmt9.setInt(1, message.getCostomID());
//							wpstmt9.executeQuery();
							conn.setAutoCommit(false);
							transStart = System.nanoTime();
							wpstmt13.setInt(1, orderkey);
//							wpstmt13.executeUpdate();
							wpstmt14.setInt(1, message.getSlskpID());
//							wpstmt14.executeUpdate();
//							conn.commit();
						}
						sleep(2);
						writeresponseTime[5] += (System.nanoTime() - transStart) / 1000;
						writeresponseTime[4]++;
						writeclientresponseTime[5] += (System.nanoTime() - clienttime)/1000;
						writeclientresponseTime[4]++;
					} catch (Exception e) {
						if(DBType!=4)
						{conn.rollback();}
						writeresponseTime[16]++;
						writeclientresponseTime[16]++;
						e.printStackTrace();
					} finally {
						conn.setAutoCommit(true);
					}
				} else if (message.getType() == 3) {
					try {
						long transStart = 0;
						ResultSet rs = null;
						if(DBType==4)
						{
							wcallpstmt17.setInt(1, message.getCostomID());
							transStart = System.nanoTime();
							rs = wcallpstmt17.executeQuery();
						}
						else
						{
							wpstmt17.setInt(1, message.getCostomID());
							transStart = System.nanoTime();
							rs = wpstmt17.executeQuery();
						}
						writeresponseTime[7] += (System.nanoTime() - transStart) / 1000;
						writeresponseTime[6]++;
						writeclientresponseTime[7] += (System.nanoTime() - clienttime)/1000;
						writeclientresponseTime[6]++;
						while (rs.next()) {
							if (random.nextFloat() < 0.5) {
								int orderkey = rs.getInt(1);
								if(DBType==4)
								{
									wcallpstmt18.setInt(1, orderkey);
									transStart = System.nanoTime();
									wcallpstmt18.executeQuery();
								}
								else
								{
									wpstmt18.setInt(1, orderkey);
									transStart = System.nanoTime();
									wpstmt18.executeQuery();
								}
								writeresponseTime[7] += (System.nanoTime() - transStart) / 1000;
								writeresponseTime[6]++;
								writeclientresponseTime[7] += (System.nanoTime() - clienttime)/1000;
								writeclientresponseTime[6]++;
								if(DBType==4)
								{
									wcallpstmt19.setInt(1, orderkey);
									transStart = System.nanoTime();
									wcallpstmt19.executeQuery();
								}
								else
								{
									wpstmt19.setInt(1, orderkey);
									transStart = System.nanoTime();
									wpstmt19.executeQuery();
								}
								writeresponseTime[7] += (System.nanoTime() - transStart) / 1000;
								writeresponseTime[6]++;
								writeclientresponseTime[7] += (System.nanoTime() - clienttime)/1000;
								writeclientresponseTime[6]++;
							}
						}
					} catch (Exception e) {
						if(DBType!=4)
						{conn.rollback();}
						writeresponseTime[16]++;
						writeclientresponseTime[16]++;
						e.printStackTrace();
					}
				}
			} else {
				if (message.getType() == 1) {
					try {
						String[] items = message.getItemID().split(",");
						double[] prices = new double[items.length];
						double allprice = 0;
						ResultSet rs = null;
						boolean flag = true;
						for (int i = 0; i < items.length; i++) {
							wpstmt5.setInt(1, Integer.valueOf((items[i].trim())));
							rs = wpstmt5.executeQuery();
							if (rs.next()) {
								prices[i] = rs.getDouble(1);
								allprice += prices[i];
							} else {
								System.out.println("select error in NOSubmitOrder!");
								continue;
							}
						}
						conn.setAutoCommit(false);
						long transStart = System.nanoTime();
						for (int i = 0; i < items.length; i++) {
							wpstmt6.setInt(1, Integer.valueOf((items[i].trim())));
							int rows = wpstmt6.executeUpdate();
							if (rows == 0) {
								conn.commit();
								writeresponseTime[16]++;
								writeclientresponseTime[16]++;
								flag = false;
								break;
							}
						}
						if (!flag) {
							// cycleGetOperator();
							return;
						}
						int orderkey = message.getOrderID() + order_i;
						// logger.info("444444!!!!!!!!!!!!!!"+ orderkey);
						wpstmt7.setInt(1, orderkey);
						wpstmt7.setInt(2, message.getCostomID());
						wpstmt7.setDouble(3, allprice);
						if(DBType==1||DBType==3){//mysql;cedar
							wpstmt7.setString(4, dateFormater.format(new Date(new java.util.Date().getTime())));
						}
						if(DBType==2){// postgres
							wpstmt7.setDate(4, new Date(new java.util.Date().getTime()));
						}
						wpstmt7.executeUpdate();
						for (int i = 0; i < items.length; i++) {
							wpstmt4.setInt(1, orderkey);
							wpstmt4.setInt(2, Integer.valueOf(items[i].trim()));
							wpstmt4.setDouble(3, prices[i]);
							wpstmt4.executeUpdate();
						}
						conn.commit();
						writeresponseTime[9] += (System.nanoTime() - transStart) / 1000;
						writeresponseTime[8]++;
						writeclientresponseTime[9] += (System.nanoTime() - clienttime)/1000;
						writeclientresponseTime[8]++;
						if (random.nextFloat() < queryRatio) {
							otherTask othertask = new otherTask(2, orderkey, message.getCostomID(), 3);
							otherOperator.addotherOperator(othertask, ((int) (timestampcount * Math.random())));
						}
						paycancelratio = random.nextFloat();
						if (paycancelratio < payRatio) {
							otherTask othertask = new otherTask(2, orderkey, message.getCostomID(), 2);
							otherOperator.addotherOperator(othertask, ((int) (timestampcount * Math.random())));
						} else if (paycancelratio < (payRatio + cancelRatio)) {
							otherTask othertask = new otherTask(2, orderkey, message.getCostomID(), 4);
							otherOperator.addotherOperator(othertask, ((int) (timestampcount * Math.random())));
						}
					} catch (Exception e) {
						conn.rollback();
						writeresponseTime[18]++;
						writeclientresponseTime[18]++;
						e.printStackTrace();
					} finally {
						conn.setAutoCommit(true);
					}
				} else if (message.getType() == 2) {
					int orderkey = message.getOrderID() + order_i;
					try {
						wpstmt9.setInt(1, message.getCostomID());
						wpstmt9.executeQuery();
						wpstmt10.setInt(1, orderkey);
						wpstmt10.executeQuery();
						conn.setAutoCommit(false);
						long transStart = System.nanoTime();
						if(DBType==1||DBType==3){//mysql;cedar
							wpstmt11.setString(1, dateFormater.format(new Date(new java.util.Date().getTime())));
						}
						if(DBType==2){// postgres
							wpstmt11.setDate(1, new Date(new java.util.Date().getTime()));
						}
						wpstmt11.setInt(2, orderkey);
						wpstmt11.executeUpdate();
						conn.commit();
						writeresponseTime[11] += (System.nanoTime() - transStart) / 1000;
						writeresponseTime[10]++;
						writeclientresponseTime[11] += (System.nanoTime() - clienttime)/1000;
						writeclientresponseTime[10]++;
					} catch (Exception e) {
						conn.rollback();
						writeresponseTime[18]++;
						writeclientresponseTime[18]++;
						e.printStackTrace();
					} finally {
						conn.setAutoCommit(true);
					}
				} else if (message.getType() == 3) {
					try {
						wpstmt17.setInt(1, message.getCostomID());
						long transStart = System.nanoTime();
						ResultSet rs = wpstmt17.executeQuery();
						writeresponseTime[15] += (System.nanoTime() - transStart) / 1000;
						writeresponseTime[14]++;
						writeclientresponseTime[15] += (System.nanoTime() - clienttime)/1000;
						writeclientresponseTime[14]++;
						while (rs.next()) {
							if (random.nextFloat() < 0.5) {
								int orderkey = rs.getInt(1);
								wpstmt18.setInt(1, orderkey);
								transStart = System.nanoTime();
								wpstmt18.executeQuery();
								writeresponseTime[15] += (System.nanoTime() - transStart) / 1000;
								writeresponseTime[14]++;
								writeclientresponseTime[15] += (System.nanoTime() - clienttime)/1000;
								writeclientresponseTime[14]++;
								wpstmt19.setInt(1, orderkey);
								transStart = System.nanoTime();
								wpstmt19.executeQuery();
								writeresponseTime[15] += (System.nanoTime() - transStart) / 1000;
								writeresponseTime[14]++;
								writeclientresponseTime[15] += (System.nanoTime() - clienttime)/1000;
								writeclientresponseTime[14]++;
							}
						}
					} catch (Exception e) {
						conn.rollback();
						writeresponseTime[18]++;
						writeclientresponseTime[18]++;
						e.printStackTrace();
					}
				} else if (message.getType() == 4) {
					int orderkey = message.getOrderID() + order_i;
					try {
						wpstmt9.setInt(1, message.getCostomID());
						wpstmt9.executeQuery();
						conn.setAutoCommit(false);
						long transStart = System.nanoTime();
						wpstmt13.setInt(1, orderkey);
						wpstmt13.executeUpdate();
						wpstmt15.setInt(1, orderkey);
						ResultSet rs = wpstmt15.executeQuery();
						while (rs.next()) {
							int itemkey = rs.getInt(1);
							int count = rs.getInt(2);
							wpstmt16.setInt(1, count);
							wpstmt16.setInt(2, itemkey);
							wpstmt16.executeUpdate();
						}
						conn.commit();
						writeresponseTime[13] += (System.nanoTime() - transStart) / 1000;
						writeresponseTime[12]++;
						writeclientresponseTime[13] += (System.nanoTime() - clienttime)/1000;
						writeclientresponseTime[12]++;
					} catch (Exception e) {
						conn.rollback();
						writeresponseTime[18]++;
						writeclientresponseTime[18]++;
						e.printStackTrace();
					} finally {
						conn.setAutoCommit(true);
					}
				}
			}
			writeresponseTimeList.set(thread_th, writeresponseTime);
			writeclientresponseTimeList.set(thread_th, writeclientresponseTime);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private synchronized int getMsgID() {
		return msgID.getAndIncrement();
	}
}
