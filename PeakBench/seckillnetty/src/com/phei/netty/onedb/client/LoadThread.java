package com.phei.netty.onedb.client;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import util.SecKillPlan;
import util.ZipfLaw;

public class LoadThread implements Runnable
{
	private volatile static AtomicInteger msgID = new AtomicInteger();
	private double seckillLoadRatio = 1;
	private double[] queryRatio = null;
	private int itemKeyRange;
	private ArrayList<ArrayList<Integer>> skItemsList = null;
	private ArrayList<ArrayList<Integer>> supplierList = null;
	private ArrayList<ArrayList<Integer>> typeList = null;
	private ZipfLaw<Integer> readzipfLaw = null;
	private Map<Integer, Integer> readmetricProp = null;
	private int hostcount = 0;
	private int limiter = 0;
	private volatile int orderKeyRange;
	private int customKeyRange;
	private ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;
	private ZipfLaw<SecKillPlan> writezipfLaw = null;
	private Map<Integer, Integer> writemetricProp = null;
	private int readWriteInterval = 0;
	private  int readworkloadtime = 0, writeworkloadtime = 0;
		
	public LoadThread(double seckillLoadRatio, double[] queryRatio, int itemKeyRange, ArrayList<ArrayList<Integer>> skItemsList,
			ArrayList<ArrayList<Integer>> supplierList, ArrayList<ArrayList<Integer>> typeList,
			int orderKeyRange, int customKeyRange, ArrayList<ArrayList<SecKillPlan>> secKillPlanList,
			ZipfLaw<Integer> readzipfLaw, ZipfLaw<SecKillPlan> writezipfLaw, Map<Integer, Integer> readmetricProp, 
			Map<Integer, Integer> writemetricProp, int readworkloadtime, int writeworkloadtime,
			int hostcount, int limiter, int readWriteInterval)
	{
		super();
		this.seckillLoadRatio = seckillLoadRatio;
		this.queryRatio = queryRatio;
		this.itemKeyRange = itemKeyRange;
		this.skItemsList = skItemsList;
		this.supplierList = supplierList;
		this.typeList = typeList;
		this.orderKeyRange = orderKeyRange;
		this.customKeyRange = customKeyRange;
		this.secKillPlanList = secKillPlanList;
		this.readzipfLaw = readzipfLaw;
		this.writezipfLaw = writezipfLaw;
		this.readmetricProp = readmetricProp;
		this.writemetricProp = writemetricProp;
		this.readworkloadtime = readworkloadtime;
		this.writeworkloadtime = writeworkloadtime;
		this.hostcount = hostcount;
		this.limiter = limiter;
		this.readWriteInterval = readWriteInterval;
	}
	public void run()
	{
		if(readworkloadtime != 0)
		{
			new Thread(new ReadLoadThread(seckillLoadRatio, queryRatio, 
					itemKeyRange, skItemsList, supplierList, typeList, readzipfLaw, 
					readmetricProp, hostcount, limiter, readworkloadtime)).start();
		}
		try{
			Thread.sleep(readWriteInterval * 1000);
		}
		catch (InterruptedException e){
			e.printStackTrace();
		}
		System.out.println("stert writeload");
		if(writeworkloadtime != 0)
		{
			System.out.println("*********write load chansheng");
			new Thread(new WriteLoadThread(seckillLoadRatio, orderKeyRange, 
					customKeyRange, itemKeyRange, secKillPlanList, writezipfLaw, 
					writemetricProp, hostcount, writeworkloadtime)).start();
		}
	}
	
	//确保msgID的唯一性，这里是单线程，没有synchronized也可以
	public synchronized static int getMsgID() {
		return msgID.getAndIncrement();
	}
}

class ReadLoadThread implements Runnable
{
	private double seckillLoadRatio = 1;
	private double[] queryRatio = null;
	private int itemKeyRange;
	private ArrayList<ArrayList<Integer>> skItemsList = null;
	private ArrayList<ArrayList<Integer>> supplierList = null;
	private ArrayList<ArrayList<Integer>> typeList = null;
	private ZipfLaw<Integer> zipfLaw = null;
	private Map<Integer, Integer> metricProp = null;
	private int hostcount = 0;
	private int limiter = 0;
	private double[] zipfLawData = null;
	private  int readworkloadtime = 0;
	
	public ReadLoadThread(double seckillLoadRatio,
			double[] queryRatio, int itemKeyRange, ArrayList<ArrayList<Integer>> skItemsList,
			ArrayList<ArrayList<Integer>> supplierList, ArrayList<ArrayList<Integer>> typeList,
			ZipfLaw<Integer> zipfLaw, Map<Integer, Integer> metricProp, int hostcount, int limiter, int readworkloadtime)
	{
		super();
		this.seckillLoadRatio = seckillLoadRatio;
		this.queryRatio = queryRatio;
		this.itemKeyRange = itemKeyRange;
		this.skItemsList = skItemsList;
		this.supplierList = supplierList;
		this.typeList = typeList;
		this.zipfLaw = zipfLaw;
		this.metricProp = metricProp;
		this.hostcount = hostcount;
		this.limiter = limiter;
		this.readworkloadtime = readworkloadtime;
	}
	
	public void run()
	{
		System.out.println("*********Roadthread****");
		zipfLawData = zipfLaw.getZipfLawData();
		new Timer().schedule(new TimerTask() { 
			int readLoad = 0;
			int count = 0;
			Random random = new Random();
			int itemkey = 0,typekey = 0,supplierkey=0;
			public void run()
			{
				readLoad = metricProp.get(count++);
				for(int i = 0; i < readLoad; i++)
				{
					MessageProto.Message.Builder Message = MessageProto.
							Message.newBuilder();
					Message.setMsgID(LoadThread.getMsgID());
					Message.setType(1);
					MessageProto.MessageRead.Builder workMessage = MessageProto.
							MessageRead.newBuilder();
					Float randomValue = random.nextFloat();
					if(randomValue < queryRatio[0]) {
						workMessage.setType(1);
						if(random.nextFloat() <= seckillLoadRatio) {
							itemkey = zipfLaw.getZipfLawValue(random, skItemsList, zipfLawData);
						} else {
							itemkey = random.nextInt(itemKeyRange);
						}
						workMessage.setItemID(itemkey);
					} else if(randomValue < queryRatio[1]) {
						workMessage.setType(2);
						typekey = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
						workMessage.setTypeID(typekey);
						workMessage.setLimite(limiter);
					} else if(randomValue < queryRatio[2]) {
						workMessage.setType(3);
						typekey = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
						workMessage.setTypeID(typekey);
						workMessage.setLimite(limiter);
					} else if(randomValue < queryRatio[3]) {
						workMessage.setType(4);
						typekey = zipfLaw.getZipfLawValue(random, typeList,zipfLawData);
						workMessage.setTypeID(typekey);
						workMessage.setLimite(limiter);
					} else {
						workMessage.setType(5);
						supplierkey = zipfLaw.getZipfLawValue(random, supplierList,zipfLawData);
						workMessage.setSuppID(supplierkey);
						workMessage.setLimite(limiter);
					}
					Message.setMessageRead(workMessage.build());
					NettyClient.WorkOrder(Message.build(), random.nextInt(hostcount));
				}
				if(count > metricProp.size()||count > readworkloadtime)
                {
                	cancel();
                }
			}
		}, 0, 1000);
		
	}
	
}
class WriteLoadThread implements Runnable
{
	private double seckillLoadRatio;
	private volatile int orderKeyRange;
	private int customKeyRange;
	private int itemKeyRange;
	private ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;
	private ZipfLaw<SecKillPlan> zipfLaw = null;
	private Map<Integer, Integer> metricProp = null;
	private int hostcount = 0;
	private double[] zipfLawData = null;
	private int writeworkloadtime = 0;
	
	public WriteLoadThread(double seckillLoadRatio,
			int orderKeyRange, int customKeyRange, int itemKeyRange, 
			ArrayList<ArrayList<SecKillPlan>> secKillPlanList, ZipfLaw<SecKillPlan> zipfLaw, 
			Map<Integer, Integer> metricProp, int hostcount, int writeworkloadtime)
	{
		super();
		this.seckillLoadRatio = seckillLoadRatio;
		this.orderKeyRange = orderKeyRange;
		this.customKeyRange = customKeyRange;
		this.itemKeyRange = itemKeyRange;
		this.secKillPlanList = secKillPlanList;
		this.zipfLaw = zipfLaw;
		this.metricProp = metricProp;
		this.hostcount = hostcount;
		this.writeworkloadtime = writeworkloadtime;
	}
	
	@Override
	public void run()
	{
		System.out.println("*********writethread****");
		zipfLawData = zipfLaw.getZipfLawData();
		new Timer().schedule(new TimerTask() { 
			int writeLoad = 0;
			int count = 0;
			Random random = new Random();
			int sckplankey = 0;
			String itemkey = null;
			SecKillPlan seckplan = new SecKillPlan();
			public void run()
			{
				writeLoad = metricProp.get(count++);
				for(int i = 0; i < writeLoad; i++)
				{
					MessageProto.Message.Builder Message = MessageProto.
							Message.newBuilder();
					Message.setMsgID(LoadThread.getMsgID());
					Message.setType(2);
					MessageProto.MessageWrite.Builder workMessage = MessageProto.
							MessageWrite.newBuilder();
					workMessage.setType(1);
					if(random.nextFloat() <= seckillLoadRatio)
					{
						workMessage.setSeckorder(1);
						seckplan = zipfLaw.getZipfLawValue(random, secKillPlanList, zipfLawData);
						sckplankey = seckplan.skpkey;
						workMessage.setSlskpID(sckplankey);
						workMessage.setItemID(String.valueOf(seckplan.itemkey));
						workMessage.setOrderID(orderKeyRange++);
						workMessage.setCostomID(random.nextInt(customKeyRange));
						Message.setMessageWrite(workMessage.build());
						NettyClient.WorkOrder(Message.build(), (sckplankey%hostcount));
					}
					else
					{
						workMessage.setSeckorder(2);
						itemkey = String.valueOf(random.nextInt(itemKeyRange));
						//一个订单随机包含多个商品
						for(int j = 0; j < random.nextInt(1); j++)
						{
							itemkey += ","+String.valueOf(random.nextInt(itemKeyRange));
						}
						workMessage.setItemID(itemkey);
						workMessage.setOrderID(orderKeyRange++);
						workMessage.setCostomID(random.nextInt(customKeyRange));
						Message.setMessageWrite(workMessage.build());
						NettyClient.WorkOrder(Message.build(), random.nextInt(hostcount));
					}
				}
				if(count > metricProp.size() || count > writeworkloadtime)
                {
                	cancel();
                }
		
			}
		}, 0, 1000);
	}
}