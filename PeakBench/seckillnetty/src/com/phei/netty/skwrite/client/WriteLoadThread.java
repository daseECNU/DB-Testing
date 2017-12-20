package com.phei.netty.skwrite.client;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import util.SecKillPlan;
import util.ZipfLaw;

public class WriteLoadThread implements Runnable
{
	private volatile static AtomicInteger msgID = new AtomicInteger();
	private int seckillReadLoad;
	private int nonseckillReadLoad;
	private volatile int orderKeyRange;
	private int customKeyRange;
	private int itemKeyRange;
	private ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;
	private ZipfLaw<SecKillPlan> zipfLaw = null;
	private Map<Integer, Integer> metricProp = null;
	private int hostcount = 0;
	private double[] zipfLawData = null;
	
	public WriteLoadThread(int seckillReadLoad, int nonseckillReadLoad,
			int orderKeyRange, int customKeyRange, int itemKeyRange, 
			ArrayList<ArrayList<SecKillPlan>> secKillPlanList, ZipfLaw<SecKillPlan> zipfLaw, 
			Map<Integer, Integer> metricProp, int hostcount)
	{
		super();
		this.seckillReadLoad = seckillReadLoad;
		this.nonseckillReadLoad = nonseckillReadLoad;
		this.orderKeyRange = orderKeyRange;
		this.customKeyRange = customKeyRange;
		this.itemKeyRange = itemKeyRange;
		this.secKillPlanList = secKillPlanList;
		this.zipfLaw = zipfLaw;
		this.metricProp = metricProp;
		this.hostcount = hostcount;
	}
	
	@Override
	public void run()
	{
		System.out.println("*********Roadthread****");
		final float skLoadRatio = (float)seckillReadLoad / (seckillReadLoad + nonseckillReadLoad);
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
					MessageWriteProto.MessageWrite.Builder workMessage = MessageWriteProto.
							MessageWrite.newBuilder();
					workMessage.setMsgID(getMsgID());
					workMessage.setType(1);
					if(random.nextFloat() <= skLoadRatio)
					{
						workMessage.setSeckorder(1);
						seckplan = zipfLaw.getZipfLawValue(random, secKillPlanList, zipfLawData);
						sckplankey = seckplan.skpkey;
						workMessage.setSlskpID(sckplankey);
						workMessage.setItemID(String.valueOf(seckplan.itemkey));
						workMessage.setOrderID(orderKeyRange++);
						workMessage.setCostomID(random.nextInt(customKeyRange));
						WLNettyClient.routeWorkOrder(workMessage.build(), (sckplankey%hostcount));
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
						WLNettyClient.routeWorkOrder(workMessage.build(), random.nextInt(hostcount));
					}
				}
				if(count > metricProp.size())
                {
                	cancel();
                }
		
			}
		}, 0, 1000);
	}
	
	private synchronized int getMsgID() {
		return msgID.getAndIncrement();
	}
}
