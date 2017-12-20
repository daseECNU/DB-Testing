package com.phei.netty.skread.client;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.log4j.Logger;
import util.Operation;
import com.phei.netty.skread.server.MessageReadReturnProto;

public class RLResultStatistic implements Runnable
{
	private static ArrayBlockingQueue<Operation<MessageReadReturnProto.MessageReadReturn>> operationQueue = null;
	private int queueSize;
	private int workloadtime = 0;
	private static Logger logger = Logger.getLogger("infoLogger"); 
	
	public RLResultStatistic(int queueSize, int workloadtime)
	{
		super();
		this.queueSize = queueSize;
		this.workloadtime = workloadtime;
		init();
	}
	public void init()
	{
		operationQueue = new ArrayBlockingQueue<Operation<MessageReadReturnProto.MessageReadReturn>>(queueSize, true);
	}
	
	public static void addOperation(Operation<MessageReadReturnProto.MessageReadReturn> operation)
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
		Operation<MessageReadReturnProto.MessageReadReturn> operation = null;
		MessageReadReturnProto.MessageReadReturn message = null;
		boolean bool = true;
		ArrayList<ArrayList<ArrayList>> resultStatistic = new ArrayList<ArrayList<ArrayList>>();
		long timestamp = 0;
		int sum = 0;
		int index = 0,presum = 0;
		float qps = 0, time = 0;
		
		for(int i=0;i<=5;i++)
		{
			resultStatistic.add(new ArrayList<ArrayList>());
			for(int j=0;j<6;j++)
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
				resultStatistic.get((int) (sum%5)).get(1).add(message.getTime1());
				resultStatistic.get((int) (sum%5)).get(2).add(message.getTime2());
				resultStatistic.get((int) (sum%5)).get(3).add(message.getTime3());
				resultStatistic.get((int) (sum%5)).get(4).add(message.getTime4());
				resultStatistic.get((int) (sum%5)).get(5).add(message.getTime5());
				if(sum>=3&&sum >presum)
				{
					for(int j=0;j<6;j++)
					{
						if(j==0)
						{
//							System.out.println("index: "+index);
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								qps += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("Qps: "+qps);
							logger.info("Qps: "+qps);
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
