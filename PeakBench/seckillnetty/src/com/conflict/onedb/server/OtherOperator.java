package com.conflict.onedb.server;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.conflict.onedb.client.MessageProto;
import com.google.common.collect.Maps;

import util.CountCounter;
import util.Operation;
import util.otherTask;

public class OtherOperator implements Runnable
{
	private Vector<Vector<otherTask>> operationQueue = null;
	private ArrayBlockingQueue<otherTask> operationCacheQueues = new ArrayBlockingQueue<otherTask>(1000000);
	private int queueSize = 0;
	private int index = 0;
	private int readwriteinterval = 0;
	private int writeDataGeneratorType = 0;
	private int writeworkloadtime = 0;
	private String writeotherworkloadcountFile = null;
	private static Map<Integer, Integer> WritemetricProp = Maps.newLinkedHashMap();
	OtherOperatorthread otherOperatorthread = null;
	private volatile static AtomicInteger msgID = new AtomicInteger();
	public OtherOperator(int queueSize, int readwriteinterval, 
			int writeDataGeneratorType, String writeotherworkloadcountFile)
	{
		super();
		this.queueSize = queueSize;
		this.writeworkloadtime = queueSize;
		this.readwriteinterval = readwriteinterval;
		this.writeDataGeneratorType = writeDataGeneratorType;
		this.writeotherworkloadcountFile = writeotherworkloadcountFile;
		init();
	}
	void init()
	{
		operationQueue =  new Vector<Vector<otherTask>>();
		otherOperatorthread = new OtherOperatorthread(queueSize);
		for(int i = 0 ; i<queueSize; i++)
		{
			operationQueue.add(new Vector<otherTask>());
		}
		new Thread(otherOperatorthread).start();
		index = 0;
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(writeotherworkloadcountFile)));
			String[] loadcount = null; 
			String sringline = null;
			while((sringline = br.readLine()) != null)
			{
				loadcount= sringline.split("=");
				WritemetricProp.put(Integer.parseInt(loadcount[0].trim()), 
						Integer.parseInt(loadcount[1].trim()));
			}
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}
		WritemetricProp = CountCounter.counter(WritemetricProp, writeworkloadtime+1);
	}
	void addotherOperator(otherTask othertask, int time)
	{
		operationQueue.get((index+time)%queueSize).add(othertask);
	}
	void addotherOperator(otherTask othertask)
	{
		try {
			operationCacheQueues.put(othertask);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void run()
	{
		if(writeDataGeneratorType == 2)
		{
			new Timer().schedule(new TimerTask() {
				int count = 0;
				int workLoad = 0;
				public void run() 
				{ 
					workLoad = WritemetricProp.get(count++);
					for(int i = 0; i < workLoad; i++)
					{
						MessageProto.Message.Builder Message = MessageProto.
								Message.newBuilder();
						Message.setMsgID(getMsgID());
						Message.setType(2);
						otherTask othertask;
						try {
							othertask = operationCacheQueues.take();
							MessageProto.MessageWrite.Builder workmessage = MessageProto.
									MessageWrite.newBuilder();
							workmessage.setType(othertask.getType());
//							workmessage.setTimestamp(System.nanoTime());
							workmessage.setOrderID(othertask.getOrderid());
							workmessage.setCostomID(othertask.getCustomid());
							if(othertask.getSeckflog()==1)
							{
								workmessage.setSlskpID(othertask.getSlskpid());
								workmessage.setSeckorder(1);
							}
							else
							{
								workmessage.setSeckorder(2);
							}
							Message.setMessageWrite(workmessage.build());
							Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(Message.build(),1,3,System.nanoTime());
							ProceseNode.getDbOperator().addOperation(operation);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (count > WritemetricProp.size()) {
						cancel();
					}
			} }, (readwriteinterval+3)*1000, 1000);
		}
		else
		{
			new Timer().schedule(new TimerTask() {
				public void run() 
				{ 
					if(operationQueue.get(index).size()>0)
					{
						Vector<otherTask> vector = operationQueue.get(index);
						otherOperatorthread.getVetorOperator(vector);
						operationQueue.add(index,new Vector<otherTask>());
					}
					index = (index+1)%queueSize;
			} }, readwriteinterval*1000, 1000);
		}
	}
	private synchronized int getMsgID() {
		return msgID.getAndIncrement();
	}
}

class OtherOperatorthread implements Runnable
{
	private ArrayBlockingQueue<Vector<otherTask>> Queue = null;
	private volatile static AtomicInteger msgID = new AtomicInteger();
	public OtherOperatorthread(int queueSize)
	{
		super();
		Queue = new ArrayBlockingQueue<Vector<otherTask>>(queueSize,true);
	}
	void getVetorOperator(Vector<otherTask> vector)
	{
		Queue.add(vector);
	}
	public void run()
	{
		while(true)
		{
			try
			{
				Vector<otherTask> vector = Queue.take();
//				System.out.println("other vector size"+vector.size());
				for(int i=0; i<vector.size();i++)
				{
					MessageProto.Message.Builder Message = MessageProto.
							Message.newBuilder();
					Message.setMsgID(getMsgID());
					Message.setType(2);
					otherTask othertask = vector.get(i);
					MessageProto.MessageWrite.Builder workmessage = MessageProto.
							MessageWrite.newBuilder();
//					workmessage.setTimestamp(System.nanoTime());
					workmessage.setType(othertask.getType());
					workmessage.setOrderID(othertask.getOrderid());
					workmessage.setCostomID(othertask.getCustomid());
					if(othertask.getSeckflog()==1)
					{
						workmessage.setSlskpID(othertask.getSlskpid());
						workmessage.setSeckorder(1);
					}
					else
					{
						workmessage.setSeckorder(2);
					}
					Message.setMessageWrite(workmessage.build());
					Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(Message.build(),1,3,System.nanoTime());
					ProceseNode.getDbOperator().addOperation(operation);
				}
			}catch (InterruptedException e){
			}
		}
	}
	private synchronized int getMsgID() {
		return msgID.getAndIncrement();
	}
	
}