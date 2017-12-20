package com.conflict.onedb.client;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

import util.SecKillPlan;
import util.ZipfLaw;

public class LoadThread implements Runnable {
	private volatile static AtomicInteger msgID = new AtomicInteger();
	private double seckillLoadRatio = 1;
	private double[] queryRatio = null;
	private int[] queryContain = null;
	private int itemKeyRange;
	private int sckplanKeyRange;
	private ArrayList<ArrayList<Integer>> skItemsList = null;
	private ArrayList<ArrayList<Integer>> supplierList = null;
	private ArrayList<ArrayList<Integer>> typeList = null;
	private ZipfLaw<Integer> readzipfLaw = null;
	private Map<Integer, Integer> ReadprimarymetricProp =null;
	private Map<Integer, Integer> ReadnonprimarymetricProp = null;
	private int hostcount = 0;
	private int limiter = 0;
	private volatile int orderKeyRange;
	private int customKeyRange;
	private ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;
	private ZipfLaw<SecKillPlan> writezipfLaw = null;
	private Map<Integer, Integer> writemetricProp = null;
	private int readWriteInterval = 0;
	private int readworkloadtime = 0, writeworkloadtime = 0;
	private float CR, skewRatio, skewDataRatio;
	private int CN = 0;
	private int writeDataGeneratorType, conflictWorkloadcount;
	private int readDataGeneratorType, readWorkloadcount, seperationType;

	public LoadThread(double seckillLoadRatio, int sckplanKeyRange, double[] queryRatio, 
			int[] queryContain, int itemKeyRange, ArrayList<ArrayList<Integer>> skItemsList, 
			ArrayList<ArrayList<Integer>> supplierList, ArrayList<ArrayList<Integer>> typeList, 
			int orderKeyRange, int customKeyRange, ArrayList<ArrayList<SecKillPlan>> secKillPlanList, 
			ZipfLaw<Integer> readzipfLaw, ZipfLaw<SecKillPlan> writezipfLaw, Map<Integer, Integer> ReadprimarymetricProp,
			Map<Integer, Integer> ReadnonprimarymetricProp, Map<Integer, Integer> writemetricProp, 
			int readworkloadtime, int writeworkloadtime, int hostcount, int limiter, 
			int readWriteInterval, float CR, int CN, float skewRatio, float skewDataRatio, int writeDataGeneratorType, 
			int conflictWorkloadcount, int readDataGeneratorType, int readWorkloadcount, int seperationType) {
		super();
		this.seckillLoadRatio = seckillLoadRatio;
		this.sckplanKeyRange = sckplanKeyRange;
		this.queryRatio = queryRatio;
		this.queryContain = queryContain;
		this.itemKeyRange = itemKeyRange;
		this.skItemsList = skItemsList;
		this.supplierList = supplierList;
		this.typeList = typeList;
		this.orderKeyRange = orderKeyRange;
		this.customKeyRange = customKeyRange;
		this.secKillPlanList = secKillPlanList;
		this.readzipfLaw = readzipfLaw;
		this.writezipfLaw = writezipfLaw;
		this.ReadprimarymetricProp = ReadprimarymetricProp;
		this.ReadnonprimarymetricProp = ReadnonprimarymetricProp;
		this.writemetricProp = writemetricProp;
		this.readworkloadtime = readworkloadtime;
		this.writeworkloadtime = writeworkloadtime;
		this.hostcount = hostcount;
		this.limiter = limiter;
		this.readWriteInterval = readWriteInterval;
		this.CR = CR;
		this.CN = CN;
		this.skewRatio = skewRatio;
		this.skewDataRatio = skewDataRatio;
		this.writeDataGeneratorType = writeDataGeneratorType;
		this.conflictWorkloadcount = conflictWorkloadcount;
		this.readDataGeneratorType = readDataGeneratorType;
		this.readWorkloadcount = readWorkloadcount;
		this.seperationType = seperationType;
	}

	public void run() {
		if (readworkloadtime != 0 || readDataGeneratorType != 0) {
			new Thread(new ReadLoadThread(seckillLoadRatio, queryRatio, queryContain, itemKeyRange, skItemsList, supplierList, typeList,
							readzipfLaw, ReadprimarymetricProp, ReadnonprimarymetricProp, hostcount, limiter, readDataGeneratorType, readWorkloadcount,
							skewRatio, skewDataRatio, sckplanKeyRange, secKillPlanList, writezipfLaw, seperationType)).start();
		}
		try {
			Thread.sleep(readWriteInterval*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("stert writeload");
		if (writeworkloadtime != 0 || writeDataGeneratorType != 0) {
			new Thread(new WriteLoadThread(seckillLoadRatio, sckplanKeyRange, orderKeyRange, customKeyRange,
					itemKeyRange, secKillPlanList, writezipfLaw, writemetricProp, hostcount, CR, CN, skewRatio,
					skewDataRatio, writeDataGeneratorType, conflictWorkloadcount)).start();
		}
	}

	// 确保msgID的唯一性，这里是单线程，没有synchronized也可以
	public synchronized static int getMsgID() {
		return msgID.getAndIncrement();
	}
}

class ReadLoadThread implements Runnable {
	private double seckillLoadRatio;
	private double[] queryRatio = null;
	private int[] queryContain = null;
	private int itemKeyRange;
	private ArrayList<ArrayList<Integer>> skItemsList = null;
	private ArrayList<ArrayList<Integer>> supplierList = null;
	private ArrayList<ArrayList<Integer>> typeList = null;
	private ZipfLaw<Integer> zipfLaw = null;
	private ZipfLaw<SecKillPlan> WzipfLaw = null;
	private Map<Integer, Integer> ReadprimarymetricProp =null;
	private Map<Integer, Integer> ReadnonprimarymetricProp = null;
	private int hostcount = 0;
	private int limiter = 0;
	private double[] zipfLawData = null;
	private int dataGeneratorType, readWorkloadcount;
	private float skewRatio, skewDataRatio;
	private int sckplanKeyRange, seperationType;
	private ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;

	public ReadLoadThread(double seckillLoadRatio, double[] queryRatio,  int[] queryContain, int itemKeyRange,
			ArrayList<ArrayList<Integer>> skItemsList, ArrayList<ArrayList<Integer>> supplierList,
			ArrayList<ArrayList<Integer>> typeList, ZipfLaw<Integer> zipfLaw,  Map<Integer, Integer> ReadprimarymetricProp,
			Map<Integer, Integer> ReadnonprimarymetricProp, int hostcount, int limiter, 
			int dataGeneratorType, int readWorkloadcount, float skewRatio, float skewDataRatio,
			int sckplanKeyRange, ArrayList<ArrayList<SecKillPlan>> secKillPlanList, 
			ZipfLaw<SecKillPlan> WzipfLaw, int seperationType) {
		super();
		this.seckillLoadRatio = seckillLoadRatio;
		this.queryRatio = queryRatio;
		this.queryContain = queryContain;
		this.itemKeyRange = itemKeyRange;
		this.skItemsList = skItemsList;
		this.supplierList = supplierList;
		this.typeList = typeList;
		this.zipfLaw = zipfLaw;
		this.ReadprimarymetricProp = ReadprimarymetricProp;
		this.ReadnonprimarymetricProp = ReadnonprimarymetricProp;
		this.hostcount = hostcount;
		this.limiter = limiter;
		this.dataGeneratorType = dataGeneratorType;
		this.readWorkloadcount = readWorkloadcount;
		this.skewRatio = skewRatio;
		this.skewDataRatio = skewDataRatio;
		this.sckplanKeyRange = sckplanKeyRange;
		this.secKillPlanList = secKillPlanList;
		this.WzipfLaw = WzipfLaw;
		this.seperationType = seperationType;
	}

	public void run() {
		System.out.println("*********Roadthread****");
		zipfLawData = zipfLaw.getZipfLawData();
		if (dataGeneratorType == 1 || dataGeneratorType == 3) {
			generateReadData(dataGeneratorType, readWorkloadcount);
		} else if (dataGeneratorType == 2) {
			if(queryContain[0]==1)
			{
				new Timer().schedule(new TimerTask() {
					int readLoad = 0;
					int count = 0;
					public void run() {
						readLoad = ReadprimarymetricProp.get(count++);
						System.out.println("!!!!!!!!!!!!!!!!!!!!!Readloadaaaaa111111****"+readLoad);
						generateReadData(4, readLoad);
						if (count >= ReadprimarymetricProp.size()) {
							cancel();
						}
					}
				}, 0, 1000);
			}
			if(queryContain[1]==1)
			{
				new Timer().schedule(new TimerTask() {
					int readLoad = 0;
					int count = 0;
					public void run() {
						readLoad = ReadnonprimarymetricProp.get(count++);
						System.out.println("!!!!!!!!!!!!!!!!!!!!!Readloadaaaaa22222222222222****"+readLoad);
						generateReadData(5, readLoad);
						if (count >= ReadnonprimarymetricProp.size()) {
							cancel();
						}
					}
				}, 0, 1000);
			}
		}
	}

	public void generateReadData(int dataGeneratorType, int readLoad) {
		Random random = new Random();
		int itemkey = 0, typekey = 0, supplierkey = 0;
		int sckplankey = 0;
		SecKillPlan seckplan = new SecKillPlan();
		for (int i = 0; i < readLoad; i++) {
			MessageProto.Message.Builder Message = MessageProto.Message.newBuilder();
			Message.setMsgID(LoadThread.getMsgID());
			Message.setType(1);
			MessageProto.MessageRead.Builder workMessage = MessageProto.MessageRead.newBuilder();
//			workMessage.setTimestamp(System.nanoTime());
			Float randomValue = random.nextFloat();
			if(dataGeneratorType == 1 || dataGeneratorType == 3){
				if (randomValue < queryRatio[0]) {
					if(seperationType == 0)
					{
						workMessage.setType(6);
						if(dataGeneratorType == 1){
							seckplan = WzipfLaw.getZipfLawValue(random, secKillPlanList, zipfLawData);
							sckplankey = seckplan.skpkey;
						}
						else if(dataGeneratorType == 3){
							workMessage.setType(6);
							if (random.nextFloat() < skewRatio) {
								sckplankey = random.nextInt((int) (sckplanKeyRange * skewDataRatio));
							} else {
								sckplankey = random.nextInt((int) (sckplanKeyRange * (1 - skewDataRatio)))
										+ (int) (sckplanKeyRange * skewDataRatio);
							}
						}
						workMessage.setSlskpID(sckplankey);
					}
					else if(seperationType == 1){
						workMessage.setType(1);
						if (random.nextFloat() <= seckillLoadRatio) {
							itemkey = random.nextInt(itemKeyRange);
//							itemkey = zipfLaw.getZipfLawValue(random, skItemsList, zipfLawData);
						} else {
							itemkey = random.nextInt(itemKeyRange);
						}
						workMessage.setItemID(itemkey);
					}
				} else if (randomValue < queryRatio[1]) {
					workMessage.setType(2);
					typekey = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
					workMessage.setTypeID(typekey);
					workMessage.setLimite(limiter);
				} else if (randomValue < queryRatio[2]) {
					workMessage.setType(3);
					typekey = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
					workMessage.setTypeID(typekey);
					workMessage.setLimite(limiter);
				} else if (randomValue < queryRatio[3]) {
					workMessage.setType(4);
					typekey = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
					workMessage.setTypeID(typekey);
					workMessage.setLimite(limiter);
				} else {
					workMessage.setType(5);
					supplierkey = zipfLaw.getZipfLawValue(random, supplierList, zipfLawData);
					workMessage.setSuppID(supplierkey);
					workMessage.setLimite(limiter);
				}
			}
			else if(dataGeneratorType == 4)
			{
				if(random.nextFloat() < seckillLoadRatio)
				{
					workMessage.setType(6);
					if (random.nextFloat() < skewRatio) {
						sckplankey = random.nextInt((int) (sckplanKeyRange * skewDataRatio));
					} else {
						sckplankey = random.nextInt((int) (sckplanKeyRange * (1 - skewDataRatio)))
								+ (int) (sckplanKeyRange * skewDataRatio);
					}
					workMessage.setSlskpID(sckplankey);
				}
				else
				{
					workMessage.setType(1);
					itemkey = random.nextInt(itemKeyRange);
					workMessage.setItemID(itemkey);
				}
			}
			else if(dataGeneratorType == 5)
			{
				if(random.nextFloat() < 0.5)
				{
					workMessage.setType(2);
					typekey = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
					workMessage.setTypeID(typekey);
					workMessage.setLimite(limiter);
				}
				else
				{
					workMessage.setType(5);
					supplierkey = zipfLaw.getZipfLawValue(random, supplierList, zipfLawData);
					workMessage.setSuppID(supplierkey);
					workMessage.setLimite(limiter);
				}
			}
			Message.setMessageRead(workMessage.build());
			NettyClient.WorkOrder(Message.build(), random.nextInt(hostcount));
		}
	}

}

class WriteLoadThread implements Runnable {
	private double seckillLoadRatio = 1;
	private volatile int orderKeyRange;
	private int customKeyRange;
	private int itemKeyRange;
	private int sckplanKeyRange;
	private ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;
	private ZipfLaw<SecKillPlan> zipfLaw = null;
	private Map<Integer, Integer> metricProp = null;
	private int hostcount = 0;
	private double[] zipfLawData = null;
	private float CR, skewRatio, skewDataRatio;
	private int CN = 0;
	private int dataGeneratorType, conflictWorkloadcount;

	public WriteLoadThread(double seckillLoadRatio, int sckplanKeyRange, int orderKeyRange, int customKeyRange,
			int itemKeyRange, ArrayList<ArrayList<SecKillPlan>> secKillPlanList, ZipfLaw<SecKillPlan> zipfLaw,
			Map<Integer, Integer> metricProp, int hostcount, float CR, int CN, float skewRatio, float skewDataRatio,
			int dataGeneratorType, int conflictWorkloadcount) {
		super();
		this.seckillLoadRatio = seckillLoadRatio;
		this.sckplanKeyRange = sckplanKeyRange;
		this.orderKeyRange = orderKeyRange;
		this.customKeyRange = customKeyRange;
		this.itemKeyRange = itemKeyRange;
		this.secKillPlanList = secKillPlanList;
		this.zipfLaw = zipfLaw;
		this.metricProp = metricProp;
		this.hostcount = hostcount;
		this.CR = CR;
		this.CN = CN;
		this.skewRatio = skewRatio;
		this.skewDataRatio = skewDataRatio;
		this.dataGeneratorType = dataGeneratorType;
		this.conflictWorkloadcount = conflictWorkloadcount;
	}

	public void run() {
		System.out.println("*********Writethread****");
		zipfLawData = zipfLaw.getZipfLawData();
		if (dataGeneratorType == 1 || dataGeneratorType == 3 || dataGeneratorType == 4) {
			generateWriteData(dataGeneratorType, conflictWorkloadcount);
		} else if (dataGeneratorType == 2) {
			new Timer().schedule(new TimerTask() {
				int writeLoad = 0;
				int count = 0;
				public void run() {
					writeLoad = metricProp.get(count++);
					System.out.println("!!!!!!!!!!!!!!!!!!!!!Writeloadaaaaa****"+writeLoad);
					generateWriteData(dataGeneratorType, writeLoad);
					if (count >= metricProp.size()) {
						cancel();
					}

				}
			}, 0, 1000);
		}
	}

	public void generateWriteData(int dataGeneratorType, int writeLoad) {
		Random random = new Random();
		int sckplankey = 0;
		String itemkey = null;
		SecKillPlan seckplan = new SecKillPlan();
		for (int i = 0; i < writeLoad; i++) {
			MessageProto.Message.Builder Message = MessageProto.Message.newBuilder();
			Message.setMsgID(LoadThread.getMsgID());
			Message.setType(2);
			MessageProto.MessageWrite.Builder workMessage = MessageProto.MessageWrite.newBuilder();
			workMessage.setType(1);
//			workMessage.setTimestamp(System.nanoTime());
			if (random.nextFloat() <= seckillLoadRatio) {
				workMessage.setSeckorder(1);
				if (dataGeneratorType == 1) {
					sckplankey = random.nextInt(sckplanKeyRange);
					workMessage.setItemID(String.valueOf(random.nextInt(itemKeyRange)));
				} else if (dataGeneratorType == 2 || dataGeneratorType == 4) {
					seckplan = zipfLaw.getZipfLawValue(random, secKillPlanList, zipfLawData);
					sckplankey = seckplan.skpkey;
					workMessage.setItemID(String.valueOf(seckplan.itemkey));
				} else if (dataGeneratorType == 3) {
					if (random.nextFloat() < skewRatio) {
						sckplankey = random.nextInt((int) (sckplanKeyRange * skewDataRatio));
					} else {
						sckplankey = random.nextInt((int) (sckplanKeyRange * (1 - skewDataRatio)))
								+ (int) (sckplanKeyRange * skewDataRatio);
					}
					workMessage.setItemID(String.valueOf(random.nextInt(itemKeyRange)));
				}
				workMessage.setSlskpID(sckplankey);
				workMessage.setOrderID(orderKeyRange++);
				workMessage.setCostomID(random.nextInt(customKeyRange));
				Message.setMessageWrite(workMessage.build());
				if (random.nextFloat() < CR) {
					Message.setConflict(CN);
					orderKeyRange += CN - 1;
				} else {
					Message.setConflict(1);
				}
				NettyClient.WorkOrder(Message.build(), (sckplankey % hostcount));
			} else {
				Message.setConflict(1);
				workMessage.setSeckorder(2);
				itemkey = String.valueOf(random.nextInt(itemKeyRange));
				// 一个订单随机包含多个商品
				for (int j = 0; j < random.nextInt(1); j++) {
					itemkey += "," + String.valueOf(random.nextInt(itemKeyRange));
				}
				workMessage.setItemID(itemkey);
				workMessage.setOrderID(orderKeyRange++);
				workMessage.setCostomID(random.nextInt(customKeyRange));
				Message.setMessageWrite(workMessage.build());
				NettyClient.WorkOrder(Message.build(), random.nextInt(hostcount));
			}
		}
	}
}