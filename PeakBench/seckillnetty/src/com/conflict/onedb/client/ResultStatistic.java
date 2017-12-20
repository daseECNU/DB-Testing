package com.conflict.onedb.client;

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import com.conflict.onedb.server.MessageReturnProto;

import util.Operation;

public class ResultStatistic implements Runnable
{
	private static ArrayBlockingQueue<Operation<MessageReturnProto.MessageReturn>> operationQueue = null;
	private int queueSize;
	private int readworkloadtime = 0;
	private int writeworkloadtime = 0;
	private int readwriteinterval = 0;
	private int gathertime = 0;
	private static Logger logger = Logger.getLogger("infoLogger");
	
	public ResultStatistic(int queueSize, int readworkloadtime, 
			int writeworkloadtime, int readwriteinterval, int gathertime)
	{
		super();
		this.queueSize = queueSize;
		this.readworkloadtime = readworkloadtime;
		this.writeworkloadtime = writeworkloadtime;
		this.readwriteinterval = readwriteinterval;
		this.gathertime = gathertime;
		init();
	}
	public void init()
	{
		operationQueue = new ArrayBlockingQueue<Operation<MessageReturnProto.MessageReturn>>(queueSize, true);	
	}
	public static void addOperation(Operation<MessageReturnProto.MessageReturn> operation)
	{
		System.out.println("*******xinxi**********");
		try
		{
			operationQueue.put(operation);
		}catch (InterruptedException e){
			e.printStackTrace();
		}
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void run()
	{
		System.out.println("*******Wltps start**********");
		Operation<MessageReturnProto.MessageReturn> operation = null;
		MessageReturnProto.MessageReturn message = null;
		boolean bool = true;
		ArrayList<ArrayList<ArrayList>> resultStatistic = new ArrayList<ArrayList<ArrayList>>();
		long timestamp = 0;
		int sum = 0;
		int index = 0,presum = 0;
		float qps = 0, rtime = 0;
		float tps = 0, wtime = 0;
		long count = 0;
		
		for(int i=0;i<=5;i++)
		{
			resultStatistic.add(new ArrayList<ArrayList>());
			for(int j=0;j<=30;j++)
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
				sum = (int)((message.getTimestamp()-timestamp)/gathertime);
				System.out.println("*******"+message.getTimestamp()+"**********"+sum);
				resultStatistic.get((int) (sum%5)).get(0).add(message.getTps());
				resultStatistic.get((int) (sum%5)).get(1).add(message.getSecksucesscount());
				resultStatistic.get((int) (sum%5)).get(2).add(message.getNosecksucesscount());
				resultStatistic.get((int) (sum%5)).get(3).add(message.getSeckfailcount());
				resultStatistic.get((int) (sum%5)).get(4).add(message.getNoseckfailcount());
				resultStatistic.get((int) (sum%5)).get(5).add(message.getQueuefailcount());
				resultStatistic.get((int) (sum%5)).get(6).add(message.getWtime1());
				resultStatistic.get((int) (sum%5)).get(7).add(message.getWtime2());
				resultStatistic.get((int) (sum%5)).get(8).add(message.getWtime3());
				resultStatistic.get((int) (sum%5)).get(9).add(message.getWtime4());
				resultStatistic.get((int) (sum%5)).get(10).add(message.getWtime5());
				resultStatistic.get((int) (sum%5)).get(11).add(message.getWtime6());
				resultStatistic.get((int) (sum%5)).get(12).add(message.getWtime7());
				resultStatistic.get((int) (sum%5)).get(13).add(message.getWtime8());
				resultStatistic.get((int) (sum%5)).get(14).add(message.getQps());
				resultStatistic.get((int) (sum%5)).get(15).add(message.getRtime1());
				resultStatistic.get((int) (sum%5)).get(16).add(message.getRtime2());
				resultStatistic.get((int) (sum%5)).get(17).add(message.getRtime3());
				resultStatistic.get((int) (sum%5)).get(18).add(message.getRtime4());
				resultStatistic.get((int) (sum%5)).get(19).add(message.getRtime5());
				resultStatistic.get((int) (sum%5)).get(20).add(message.getRtime6());
				resultStatistic.get((int) (sum%5)).get(21).add(message.getWctime1());
				resultStatistic.get((int) (sum%5)).get(22).add(message.getWctime2());
				resultStatistic.get((int) (sum%5)).get(23).add(message.getWctime3());
				resultStatistic.get((int) (sum%5)).get(24).add(message.getWctime4());
				resultStatistic.get((int) (sum%5)).get(25).add(message.getRctime1());
				resultStatistic.get((int) (sum%5)).get(26).add(message.getRctime2());
				resultStatistic.get((int) (sum%5)).get(27).add(message.getRctime3());
				resultStatistic.get((int) (sum%5)).get(28).add(message.getRctime4());
				resultStatistic.get((int) (sum%5)).get(29).add(message.getRctime5());
				resultStatistic.get((int) (sum%5)).get(30).add(message.getRctime6());
				if(sum>=3&&sum >presum)
				{
					for(int j=0;j<14;j++)
					{
						if(j==0)
						{
							System.out.println("write start~");
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								tps += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("write Tps: "+tps);
							logger.info("write Tps: "+tps);
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
								wtime += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("write time"+(j-5)+": "+wtime / resultStatistic.get(index).get(j).size());
							logger.info("write time"+(j-5)+": "+wtime / resultStatistic.get(index).get(j).size());
							wtime = 0;
						}
						resultStatistic.get(index).get(j).clear();
					}
					for(int j=14;j<=20;j++)
					{
						if(j==14)
						{
							System.out.println("read start~");
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								qps += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("read Qps: "+qps);
							logger.info("read Qps: "+qps);
						}
						else
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								rtime += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("read time"+(j-14)+": "+rtime / resultStatistic.get(index).get(j).size());
							logger.info("read time"+(j-14)+": "+rtime / resultStatistic.get(index).get(j).size());
							rtime = 0;
						}
						resultStatistic.get(index).get(j).clear();
					}
					for(int j=21;j<=30;j++)
					{
						if(j>=21&&j<=24)
						{
							System.out.println("client time start~");
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								rtime += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("write client time"+(j-20)+": "+rtime / resultStatistic.get(index).get(j).size());
							logger.info("write client time"+(j-20)+": "+rtime / resultStatistic.get(index).get(j).size());
							rtime = 0;
						}
						else
						{
							for(int k =0 ;k<resultStatistic.get(index).get(j).size();k++)
							{
								rtime += (float) resultStatistic.get(index).get(j).get(k); 
							}
							System.out.println("read client time"+(j-24)+": "+rtime / resultStatistic.get(index).get(j).size());
							logger.info("read client time"+(j-24)+": "+rtime / resultStatistic.get(index).get(j).size());
							rtime = 0;
						}
						resultStatistic.get(index).get(j).clear();
					}
					presum = sum;
					index = (index+1)%5;
					if(sum >(readwriteinterval + writeworkloadtime) && tps<=0 && qps<=0)
						break;
					tps = 0;
					qps = 0;
				}	
			}catch (InterruptedException e){
				e.printStackTrace();
			}
		}
		
	}
}
