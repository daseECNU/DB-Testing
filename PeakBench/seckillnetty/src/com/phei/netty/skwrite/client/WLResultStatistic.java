package com.phei.netty.skwrite.client;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;
import com.phei.netty.skwrite.server.MessageWriteReturnProto;

import util.Operation;

public class WLResultStatistic implements Runnable
{
	private static ArrayBlockingQueue<Operation<MessageWriteReturnProto.MessageWriteReturn>> operationQueue = null;
	private int queueSize;
	private int workloadtime = 0;
	private static Logger logger = Logger.getLogger("infoLogger"); 
	
	public WLResultStatistic(int queueSize, int workloadtime)
	{
		super();
		this.queueSize = queueSize;
		this.workloadtime = workloadtime;
		init();
	}
	public void init()
	{
		operationQueue = new ArrayBlockingQueue<Operation<MessageWriteReturnProto.MessageWriteReturn>>(queueSize, true);
	}
	
	public static void addOperation(Operation<MessageWriteReturnProto.MessageWriteReturn> operation)
	{
		try
		{
			operationQueue.put(operation);
		}catch (InterruptedException e){
			e.printStackTrace();
		}
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void run()
	{
		System.out.println("*******Wltps start**********");
		Operation<MessageWriteReturnProto.MessageWriteReturn> operation = null;
		MessageWriteReturnProto.MessageWriteReturn message = null;
		boolean bool = true;
		ArrayList<ArrayList<ArrayList>> resultStatistic = new ArrayList<ArrayList<ArrayList>>();
		long timestamp = 0;
		int sum = 0;
		int index = 0,presum = 0;
		float qps = 0, time = 0;
		long count = 0;
		
		for(int i=0;i<=5;i++)
		{
			resultStatistic.add(new ArrayList<ArrayList>());
			for(int j=0;j<14;j++)
			{
				resultStatistic.get(i).add(new ArrayList());
			}
		}
		while (true)
		{
			try
			{
				operation = operationQueue.take();
				message = operation.message;
				if(bool)
				{
					timestamp = message.getTimestamp();
					bool = false;
				}
				sum = (int)(message.getTimestamp()-timestamp);
//				System.out.println("*******"+message.getTimestamp()+"**********"+timestamp);
				resultStatistic.get((int) (sum%5)).get(0).add(message.getTps());
				resultStatistic.get((int) (sum%5)).get(1).add(message.getSecksucesscount());
				resultStatistic.get((int) (sum%5)).get(2).add(message.getNosecksucesscount());
				resultStatistic.get((int) (sum%5)).get(3).add(message.getSeckfailcount());
				resultStatistic.get((int) (sum%5)).get(4).add(message.getNoseckfailcount());
				resultStatistic.get((int) (sum%5)).get(5).add(message.getQueuefailcount());
				resultStatistic.get((int) (sum%5)).get(6).add(message.getTime1());
				resultStatistic.get((int) (sum%5)).get(7).add(message.getTime2());
				resultStatistic.get((int) (sum%5)).get(8).add(message.getTime3());
				resultStatistic.get((int) (sum%5)).get(9).add(message.getTime4());
				resultStatistic.get((int) (sum%5)).get(10).add(message.getTime5());
				resultStatistic.get((int) (sum%5)).get(11).add(message.getTime6());
				resultStatistic.get((int) (sum%5)).get(12).add(message.getTime7());
				resultStatistic.get((int) (sum%5)).get(13).add(message.getTime8());
				if(sum>=3&&sum >presum)
				{
					for(int j=0;j<14;j++)
					{
						if(j==0)
						{
//							System.out.println("index: "+index);
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								qps += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("Tps: "+qps);
							logger.info("Tps: "+qps);
						}
						else if(j==1)
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								count += (long) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("Secksucesscount: "+count);
							logger.info("Secksucesscount: "+count);
							count = 0;
						}
						else if(j==2)
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								count += (long) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("noSecksucesscount: "+count);
							logger.info("noSecksucesscount: "+count);
							count = 0;
						}
						else if(j==3)
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								count += (long) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("Seckfailcount: "+count);
							logger.info("Seckfailcount: "+count);
							count = 0;
						}
						else if(j==4)
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								count += (long) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("noSeckfailcount: "+count);
							logger.info("noSeckfailcount: "+count);
							count = 0;
						}
						else if(j==5)
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								count += (long) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("queuefailcount: "+count);
							logger.info("queuefailcount: "+count);
							count = 0;
						}
						else
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								time += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("time"+j+": "+time / resultStatistic.get(index).get(j).size());
							logger.info("time"+j+": "+time / resultStatistic.get(index).get(j).size());
							time = 0;
						}
						resultStatistic.get(index).get(j).clear();
					}
					presum = sum;
					index = (index+1)%5;
					if(sum > workloadtime && qps<=0)
						break;
					qps = 0;
				}	
			}catch (InterruptedException e){
				e.printStackTrace();
			}
		}
		
	}
}
