package com.phei.netty.skread.server;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import com.phei.netty.skread.client.MessageReadProto;

import util.DBConnector;
import util.Operation;
import util.RanDataGene;


public class RLDBOperator
{
	private int threadNum;
	private int queueSize;
	private DBConnector dbConnector = null;
	private ArrayList<ArrayBlockingQueue<Operation<MessageReadProto.MessageRead>>> operationQueues = null;
	private CopyOnWriteArrayList<long[]> responseTimeList = new CopyOnWriteArrayList<long[]>();
	public long gathertime = 0;
	public int timestampcount = 0;
	
	public RLDBOperator(int threadNum, int queueSize, DBConnector dbConnector,
			long gathertime, int timestampcount)
	{
		super();
		this.threadNum = threadNum;
		this.queueSize = queueSize;
		this.dbConnector = dbConnector;
		this.gathertime = gathertime;
		this.timestampcount = timestampcount;
		init();
	}
	private void init()
	{ 
		operationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageReadProto.MessageRead>>>(threadNum);
		for (int i = 0; i < threadNum; i++)
			operationQueues.add(new ArrayBlockingQueue<Operation<MessageReadProto.MessageRead>>(queueSize, true));
		for(int i = 0; i < threadNum; i++) {
			responseTimeList.add(new long[0]);
		}
		for (int i = 0; i < threadNum; i++)
		{
			try
			{
				new Thread(new DBOperatorThread(operationQueues.get(i), responseTimeList, 
						dbConnector.getConnection(), i)).start();
			}
			catch (FileNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		new Timer().schedule(new TimerTask() { 
            MessageReadReturnProto.MessageReadReturn.Builder workMessage = 
            MessageReadReturnProto.MessageReadReturn.newBuilder();
			long time = 0, count = 0, allcount = 0, lastallcount = 0;
			int timestamp = 0;
            public void run() {  
            	workMessage.setMsgID(timestamp++);
            	workMessage.setTimestamp(System.currentTimeMillis()/1000);
                for(int i = 0; i < 10; i+=2) 
                {
        			time = 0; count = 0;
        			for(int j = 0; j < threadNum ; j++) 
        			{
        				if(responseTimeList.get(j).length!=0)
        				{
        					time += responseTimeList.get(j)[i];
        					count += responseTimeList.get(j)[i + 1];
        				}
        			}
        			
    				if((i / 2 + 1)==1)
    				{	
    					if(count!=0)
    						workMessage.setTime1(((float)time / count));
    					else
    						workMessage.setTime1(0);
    				}
        			else if((i / 2 + 1)==2)
        			{	
        				if(count!=0)
    						workMessage.setTime2(((float)time / count));
    					else
    						workMessage.setTime2(0);
        			}
        			else if((i / 2 + 1)==3)
        			{
        				if(count!=0)
    						workMessage.setTime3(((float)time / count));
    					else
    						workMessage.setTime3(0);
        			}
        			else if((i / 2 + 1)==4)
        			{
        				if(count!=0)
    						workMessage.setTime4(((float)time / count));
    					else
    						workMessage.setTime4(0);
        			}
        			else if((i / 2 + 1)==5)
        			{
        				if(count!=0)
    						workMessage.setTime5(((float)time / count));
    					else
    						workMessage.setTime5(0);
        			}
        			allcount += count;
        		}
//                System.out.println("*******"+(allcount - lastallcount)+"*****");
                workMessage.setTps((float)(allcount - lastallcount)*1000/gathertime);
                RLReturnNettyClient.routeWorkOrder(workMessage.build());

                if(timestamp > timestampcount && allcount == lastallcount)
                {
                	cancel();
                }
                lastallcount = allcount;
                allcount = 0;
            }  
        }, 5000, gathertime);
	}
	public void addOperation(Operation<MessageReadProto.MessageRead> operation)
	{
		try
		{
			operationQueues.get((int) (operationQueues.size() * Math.random())).put(operation);
		}catch (InterruptedException e){
			e.printStackTrace();
		}
	}

}

class DBOperatorThread implements Runnable
{
	private ArrayBlockingQueue<Operation<MessageReadProto.MessageRead>> operationQueue = null;
	private Connection conn = null;
	private PreparedStatement pstmt1 = null, pstmt2 = null, pstmt3 = null, pstmt4 = null, pstmt5 = null;
	private Statement pst = null;
	private CopyOnWriteArrayList<long[]> responseTimeList = null;
	private int thread_th = 0;
	
	public DBOperatorThread(ArrayBlockingQueue<Operation<MessageReadProto.MessageRead>> operationQueue, 
			CopyOnWriteArrayList<long[]> responseTimeList, Connection conn, int thread_th)
	{
		super();
		this.operationQueue = operationQueue;
		this.responseTimeList = responseTimeList;
		this.conn = conn;
		this.thread_th = thread_th;
		try
		{
			init();
		}catch (SQLException e){
			e.printStackTrace();
		}
	}
	private void init() throws SQLException
	{
		pst = conn.createStatement();
	    pst.execute("set @@session.ob_query_timeout=9000000000;");
		pstmt1 = conn.prepareStatement("select * from item where itemkey = ?");
		pstmt2 = conn.prepareStatement("select * from item where type = ? limit ?");
		pstmt3 = conn.prepareStatement("select * from item where type = ? and name like ? limit ?");
		pstmt4 = conn.prepareStatement("select * from item where type = ? and price between ? and ? limit ?" );
		pstmt5 = conn.prepareStatement("select * from item where suppkey = ? limit ?" );
	}
	@Override
	public void run()
	{
		long[] responseTime = new long[10];
		int count1 = 0, count2 = 0, count3 = 0, count4 = 0, count5 = 0;
		long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0;
		Random random = new Random();
		try
		{
			while (true)
			{
				Operation<MessageReadProto.MessageRead> operation = null;
				MessageReadProto.MessageRead message = null;
				operation = operationQueue.take();
				message = operation.message;
//				System.out.println("*******"+message.getItemID());
				if (message.getType() == 1)
				{
					pstmt1.setInt(1, message.getItemID());
					long start = System.nanoTime();
					pstmt1.executeQuery();
					time1 += (System.nanoTime() - start) / 1000;
					count1++;
				}
				else if (message.getType() == 2)
				{
					pstmt2.setInt(1, message.getTypeID());
					pstmt2.setInt(2, message.getLimite());
					long start = System.nanoTime();
					pstmt2.executeQuery();
					time2 += (System.nanoTime() - start) / 1000;
					count2++;
				}
				else if (message.getType() == 3)
				{
					pstmt3.setInt(1, message.getTypeID());
					pstmt3.setString(2, RanDataGene.getChar() + RanDataGene.getChar() + "%");
					pstmt3.setInt(3, message.getLimite());
					long start = System.nanoTime();
					pstmt3.executeQuery();
					time3 += (System.nanoTime() - start) / 1000;
					count3++;
				}
				else if (message.getType() == 4)
				{
					float price1 = RanDataGene.getInteger(1, 500) - random.nextFloat();
					float price2 = price1 + RanDataGene.getInteger(1, 500);
					pstmt4.setInt(1, message.getTypeID());
					pstmt4.setFloat(2, price1);
					pstmt4.setFloat(3, price2);
					pstmt4.setInt(4, message.getLimite());
					long start = System.nanoTime();
					pstmt4.executeQuery();
					time4 += (System.nanoTime() - start) / 1000;
					count4++;
				}
				else if (message.getType() == 5)
				{
					pstmt5.setInt(1, message.getSuppID());
					pstmt5.setInt(2, message.getLimite());
					long start = System.nanoTime();
					pstmt5.executeQuery();
					time5 += (System.nanoTime() - start) / 1000;
					count5++;
				}
				responseTime [0] = time1;
				responseTime [1] = count1;
				responseTime [2] = time2;
				responseTime [3] = count2;
				responseTime [4] = time3;
				responseTime [5] = count3;
				responseTime [6] = time4;
				responseTime [7] = count4;
				responseTime [8] = time5;
				responseTime [9] = count5;
				responseTimeList.set(thread_th ,responseTime);
			}
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			try {
				if(pstmt1 != null)	pstmt1.close();
				if(pstmt2 != null)	pstmt2.close();
				if(pstmt3 != null)	pstmt3.close();
				if(pstmt4 != null)	pstmt4.close();
				if(pstmt5 != null)	pstmt5.close();
				if(conn != null)	conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}
