package com.phei.netty.onedb.server;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.phei.netty.onedb.client.MessageProto;

import util.Operation;
import util.otherTask;

public class OtherOperator implements Runnable
{
	private Vector<Vector<otherTask>> operationQueue = null;
	private int queueSize = 0;
	private int index = 0;
	private int readwriteinterval = 0;
	OtherOperatorthread otherOperatorthread = null;
	public OtherOperator(int queueSize, int readwriteinterval)
	{
		super();
		this.queueSize = queueSize;
		this.readwriteinterval = readwriteinterval;
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
	}
	void addotherOperator(otherTask othertask, int time)
	{
		operationQueue.get((index+time)%queueSize).add(othertask);
	}

	
	public void run()
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
					Operation<MessageProto.Message> operation = new Operation<MessageProto.Message>(Message.build());
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