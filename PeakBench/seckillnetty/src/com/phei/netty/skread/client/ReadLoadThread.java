package com.phei.netty.skread.client;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import util.ZipfLaw;

public class ReadLoadThread implements Runnable
{
	//确保msgID的唯一性
		private volatile static AtomicInteger msgID = new AtomicInteger();
		
		private int seckillReadLoad;
		private int nonseckillReadLoad;
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
		
		public ReadLoadThread(int seckillReadLoad, int nonseckillReadLoad,
				double[] queryRatio, int itemKeyRange, ArrayList<ArrayList<Integer>> skItemsList,
				ArrayList<ArrayList<Integer>> supplierList, ArrayList<ArrayList<Integer>> typeList,
				ZipfLaw<Integer> zipfLaw, Map<Integer, Integer> metricProp, int hostcount, int limiter)
		{
			super();
			this.seckillReadLoad = seckillReadLoad;
			this.nonseckillReadLoad = nonseckillReadLoad;
			this.queryRatio = queryRatio;
			this.itemKeyRange = itemKeyRange;
			this.skItemsList = skItemsList;
			this.supplierList = supplierList;
			this.typeList = typeList;
			this.zipfLaw = zipfLaw;
			this.metricProp = metricProp;
			this.hostcount = hostcount;
			this.limiter = limiter;
		}
		
		public void run()
		{
			System.out.println("*********Roadthread****");
			final float skLoadRatio = (float)seckillReadLoad / (seckillReadLoad + nonseckillReadLoad);
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
						MessageReadProto.MessageRead.Builder workMessage = MessageReadProto.
								MessageRead.newBuilder();
						workMessage.setMsgID(getMsgID());
						Float randomValue = random.nextFloat();
						if(randomValue < queryRatio[0]) {
							workMessage.setType(1);
							if(random.nextFloat() <= skLoadRatio) {
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
						RLNettyClient.routeWorkOrder(workMessage.build(), random.nextInt(hostcount));
					}
					if(count > metricProp.size())
	                {
	                	cancel();
	                }
				}
			}, 0, 1000);
			
		}
		
		//确保msgID的唯一性，这里是单线程，没有synchronized也可以
		private synchronized int getMsgID() {
			return msgID.getAndIncrement();
		}

}
