package com.phei.netty.skwrite.server;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.phei.netty.skwrite.client.MessageWriteProto;

import util.Operation;
import util.otherTask;

public class OtherOperator implements Runnable
{
	private Vector<Vector<otherTask>> operationQueue = null;
	private int queueSize = 0;
	private int index = 0;
	OtherOperatorthread otherOperatorthread = null;
	public OtherOperator(int queueSize)
	{
		super();
		this.queueSize = queueSize;
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
		} }, 1000, 1000);
		
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
					otherTask othertask = vector.get(i);
					MessageWriteProto.MessageWrite.Builder message = MessageWriteProto.
							MessageWrite.newBuilder();
					message.setMsgID(getMsgID());
					message.setType(othertask.getType());
					message.setOrderID(othertask.getOrderid());
					message.setCostomID(othertask.getCustomid());
					if(othertask.getSeckflog()==1)
					{
						message.setSlskpID(othertask.getSlskpid());
						message.setSeckorder(1);
					}
					else
					{
						message.setSeckorder(2);
					}
					Operation<MessageWriteProto.MessageWrite> operation = new Operation<MessageWriteProto.MessageWrite>(message.build());
					WLProceseNode.getDbOperator().addOperation(operation);
				}
			}catch (InterruptedException e){
			}
		}
	}
	private synchronized int getMsgID() {
		return msgID.getAndIncrement();
	}
	
}