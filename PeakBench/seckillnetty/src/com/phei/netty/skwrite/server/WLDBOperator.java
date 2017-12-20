package com.phei.netty.skwrite.server;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import util.DBConnector;
import util.Operation;
import util.SeckillItem;
import util.otherTask;

import com.phei.netty.skwrite.client.MessageWriteProto;

public class WLDBOperator
{
	private int threadNum;
	private int queueSize;
	private DBConnector dbConnector = null;
	private ArrayList<ArrayBlockingQueue<Operation<MessageWriteProto.MessageWrite>>> operationQueues = null;
	private CopyOnWriteArrayList<long[]> responseTimeList = new CopyOnWriteArrayList<long[]>();
	private ArrayList<HashMap<Integer, Integer>> dbDataList = null;
	private ArrayList<SeckillItem> dbData = null;
	private HashMap<Integer, ArrayList<Integer>> hashselect = new HashMap<Integer, ArrayList<Integer>>();
	private int basenum = 0;
	public long gathertime = 0;
	public int timestampcount = 0;
	public boolean hashflag = true;
	public OtherOperator otherOperator = null;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	
	public WLDBOperator(int threadNum, int queueSize, DBConnector dbConnector,
			ArrayList<SeckillItem> dbData, int basenum, long gathertime, int timestampcount,
			float payRatio, float cancelRatio,float queryRatio)
	{
		super();
		this.threadNum = threadNum;
		this.queueSize = queueSize;
		this.dbConnector = dbConnector;
		this.dbData = dbData;
		this.basenum = basenum;
		this.gathertime = gathertime;
		this.timestampcount = timestampcount;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		init();
	}
	private void init()
	{ 
		operationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageWriteProto.MessageWrite>>>(threadNum);
		dbDataList = new ArrayList<HashMap<Integer, Integer>>(threadNum);
		for(int i = 0; i < threadNum; i++){
			operationQueues.add(new ArrayBlockingQueue<Operation<MessageWriteProto.MessageWrite>>(queueSize, true));
		}
		for(int i = 0; i < threadNum; i++){
			dbDataList.add(new HashMap<Integer, Integer>());
		}
		for(int i = 0; i < threadNum; i++) {
			responseTimeList.add(new long[0]);
		}
		createHashmap();
		otherOperator = new OtherOperator(timestampcount);
		new Thread(otherOperator).start();
		
		for (int i = 0; i < threadNum; i++)
		{
			try
			{
				new Thread(new DBOperatorThread(operationQueues.get(i), dbDataList.get(i), responseTimeList, 
						dbConnector.getConnection(), i, otherOperator, timestampcount,
						payRatio, cancelRatio, queryRatio)).start();
			}
			catch (FileNotFoundException e)
			{
				e.printStackTrace();
			}
		}
		new Timer().schedule(new TimerTask() { 
            MessageWriteReturnProto.MessageWriteReturn.Builder workMessage = 
            MessageWriteReturnProto.MessageWriteReturn.newBuilder();
			long secktime = 0, seckcount = 0, seckallcount = 0, secklastallcount = 0;
			long nosecktime = 0, noseckcount = 0, noseckallcount = 0, nosecklastallcount = 0;
			long seckallfailcount = 0, secklastallfailcount = 0,
				noseckallfailcount = 0, nosecklastallfailcount = 0,
				seckallfailqueuecount = 0, secklastallfailqueuecount = 0;
			int timestamp = 0;
            public void run() {  
            	workMessage.setMsgID(timestamp++);
            	workMessage.setTimestamp(System.currentTimeMillis()/1000);
                for(int i = 0; i < 8; i+=2) 
                {
        			secktime = 0; seckcount = 0;
        			for(int j = 0; j < threadNum ; j++) 
        			{
        				if(responseTimeList.get(j).length!=0)
        				{
        					seckcount += responseTimeList.get(j)[i];
        					secktime += responseTimeList.get(j)[i + 1];
        				}
        			}        			
    				if((i / 2 + 1)==1)
    				{	
    					if(seckcount!=0)
    						workMessage.setTime1(((float)secktime / seckcount));
    					else
    						workMessage.setTime1(0);
    				}
        			else if((i / 2 + 1)==2)
        			{	
        				if(seckcount!=0)
    						workMessage.setTime2(((float)secktime / seckcount));
    					else
    						workMessage.setTime2(0);
        			}
        			else if((i / 2 + 1)==3)
        			{
        				if(seckcount!=0)
    						workMessage.setTime3(((float)secktime / seckcount));
    					else
    						workMessage.setTime3(0);
        			}
        			else if((i / 2 + 1)==4)
        			{
        				if(seckcount!=0)
    						workMessage.setTime4(((float)secktime / seckcount));
    					else
    						workMessage.setTime4(0);
        			}
    				seckallcount += seckcount;
        		}
                for(int i = 8; i < 16; i+=2) 
                {
        			nosecktime = 0; noseckcount = 0;
        			for(int j = 0; j < threadNum ; j++) 
        			{
        				if(responseTimeList.get(j).length!=0)
        				{
        					noseckcount += responseTimeList.get(j)[i];
        					nosecktime += responseTimeList.get(j)[i + 1];
        				}
        			}        			
    				if((i / 2 + 1)==5)
    				{	
    					if(noseckcount!=0)
    						workMessage.setTime5(((float)nosecktime / noseckcount));
    					else
    						workMessage.setTime5(0);
    				}
        			else if((i / 2 + 1)==6)
        			{	
        				if(noseckcount!=0)
    						workMessage.setTime6(((float)nosecktime / noseckcount));
    					else
    						workMessage.setTime6(0);
        			}
        			else if((i / 2 + 1)==7)
        			{
        				if(noseckcount!=0)
    						workMessage.setTime7(((float)nosecktime / noseckcount));
    					else
    						workMessage.setTime7(0);
        			}
        			else if((i / 2 + 1)==8)
        			{
        				if(noseckcount!=0)
    						workMessage.setTime8(((float)nosecktime / noseckcount));
    					else
    						workMessage.setTime8(0);
        			}
    				noseckallcount += noseckcount;
        		}
                for(int j = 0; j < threadNum ; j++) 
    			{
    				if(responseTimeList.get(j).length!=0)
    				{
    					seckallfailcount += responseTimeList.get(j)[16];
    				}
    			} 
                for(int j = 0; j < threadNum ; j++) 
    			{
    				if(responseTimeList.get(j).length!=0)
    				{
    					seckallfailqueuecount += responseTimeList.get(j)[17];
    				}
    			} 
                for(int j = 0; j < threadNum ; j++) 
    			{
    				if(responseTimeList.get(j).length!=0)
    				{
    					noseckallfailcount += responseTimeList.get(j)[18];
    				}
    			}
//                System.out.println("*******"+(seckallcount+noseckallcount-secklastallcount-nosecklastallcount)+"*****");
                workMessage.setSecksucesscount(seckallcount - secklastallcount);
                workMessage.setNosecksucesscount(noseckallcount - nosecklastallcount);
                workMessage.setSeckfailcount(seckallfailcount - secklastallfailcount);
                workMessage.setNoseckfailcount(noseckallfailcount - nosecklastallfailcount);
                workMessage.setQueuefailcount(seckallfailqueuecount - secklastallfailqueuecount);
                workMessage.setTps((float)(seckallcount+noseckallcount-secklastallcount-nosecklastallcount)*1000/gathertime);
                WLReturnNettyClient.routeWorkOrder(workMessage.build());
                
                if(timestamp > timestampcount && seckallcount == secklastallcount 
                	&& noseckallcount == nosecklastallcount && seckallfailcount == secklastallfailcount
                	&& noseckallfailcount == nosecklastallfailcount &&
                	seckallfailqueuecount == secklastallfailqueuecount)
                {
                	cancel();
                }
                secklastallcount = seckallcount;
                seckallcount = 0;
                nosecklastallcount = noseckallcount;
                noseckallcount = 0;
                secklastallfailcount = seckallfailcount;
                seckallfailcount = 0;
                nosecklastallfailcount = noseckallfailcount;
                noseckallfailcount = 0;
                secklastallfailqueuecount = seckallfailqueuecount;
                seckallfailqueuecount = 0;
            }  
        }, 5000, gathertime);
	}
	
	public void createHashmap()
	{
		SeckillItem item = new SeckillItem();
		if(dbData.size()>=threadNum)
		{
			hashflag = true;
			for(int i = 0;i < dbData.size();i++)
			{
				item = dbData.get(i);
//				dbDataList.get((item.skpkey/basenum)%threadNum).put(item.skpkey, item.plancount);
				dbDataList.get((item.skpkey)%threadNum).put(item.skpkey, item.plancount);
			}
		}
		else
		{
			hashflag = false;
			Collections.sort(dbData);
			int divi = threadNum / dbData.size();
			int residue = threadNum % dbData.size();
			int num = 0;
			for(int i= 0; i < residue; i++)
			{
				ArrayList<Integer> arr = new ArrayList<Integer>();
				arr.add(num);
				item = dbData.get(i);
				dbDataList.get(num++).put(item.skpkey, item.plancount);
				hashselect.put(item.skpkey, arr);
			}
			for(int i = residue; i < dbData.size(); i++)
			{
				ArrayList<Integer> arr = new ArrayList<Integer>();
				item = dbData.get(i);
				for(int j = 0; j < divi; j++)
				{
					arr.add(num);
					if(j==0)
						dbDataList.get(num++).put(item.skpkey, (item.plancount/divi) +(item.plancount%divi));
					else
						dbDataList.get(num++).put(item.skpkey, item.plancount/divi);
				}
				hashselect.put(item.skpkey, arr);
			}
		}
		
	}
	
	public int getIndex(int id)
	{
		ArrayList<Integer> arr = hashselect.get(id);
		return arr.get((int) (arr.size()* Math.random()));
	}
	
	public void addOperation(Operation<MessageWriteProto.MessageWrite> operation)
	{
		try
		{
			if(operation.message.getSeckorder() == 1)
			{
				if(hashflag)
				{
//					operationQueues.get((operation.message.getSlskpID()/basenum)%threadNum).put(operation);
					operationQueues.get((operation.message.getSlskpID())%threadNum).put(operation);
				}
				else
				{
					operationQueues.get(getIndex(operation.message.getSlskpID())).put(operation);
				}
			}
			else
			{
				operationQueues.get((int) (operationQueues.size() * Math.random())).put(operation);
			}
		}catch (InterruptedException e){
			e.printStackTrace();
		}
	}
}

class DBOperatorThread implements Runnable
{
	private ArrayBlockingQueue<Operation<MessageWriteProto.MessageWrite>> operationQueue = null;
	private HashMap<Integer, Integer> data = null;
	private Connection conn = null;
	private PreparedStatement pstmt1 = null, pstmt2 = null, pstmt3 = null, pstmt4 = null, pstmt5 = null,
				pstmt6 = null, pstmt7 = null, pstmt9 = null, pstmt10 = null, 
				pstmt11 = null, pstmt12 = null, pstmt13 = null, pstmt14 = null,pstmt15 = null,
				pstmt16 = null, pstmt17 = null, pstmt18 = null, pstmt19 = null, pstmt20 = null;
	private Statement pst = null;
	private CopyOnWriteArrayList<long[]> responseTimeList = null;
	private int thread_th = 0;
	public OtherOperator otherOperator = null;
	public int timestampcount = 0;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	
	public DBOperatorThread(ArrayBlockingQueue<Operation<MessageWriteProto.MessageWrite>> operationQueue,
			HashMap<Integer, Integer> data,
			CopyOnWriteArrayList<long[]> responseTimeList, Connection conn, 
			int thread_th, OtherOperator otherOperator, int timestampcount,
			float payRatio, float cancelRatio,float queryRatio)
	{
		super();
		this.operationQueue = operationQueue;
		this.data = data;
		this.responseTimeList = responseTimeList;
		this.conn = conn;
		this.thread_th = thread_th;
		this.otherOperator = otherOperator;
		this.timestampcount = timestampcount;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
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
	    //submit order seckill
	    pstmt1 = conn.prepareStatement("select sl_price from seckillplan where sl_skpkey = ?");
		pstmt2 = conn.prepareStatement("update seckillplan set sl_skpcount = sl_skpcount + 1 where sl_skpkey = ? and sl_skpcount < sl_plancount");
		pstmt3 = conn.prepareStatement("insert into orders (o_orderkey, o_custkey, o_skpkey, o_price, o_orderdate, o_state) values (?, ?, ?, ?, ?, 0)");
		pstmt4 = conn.prepareStatement("insert into orderitem (oi_orderkey, oi_itemkey, oi_count, oi_price) values (?, ?, 1, ?)");
		//submit order noseckill
		pstmt5 = conn.prepareStatement("select i_price from item where i_itemkey = ?");
		pstmt6 = conn.prepareStatement("update item set i_count = i_count - 1 where i_itemkey = ? and i_count >= 1");
		pstmt7 = conn.prepareStatement("insert into orders (o_orderkey, o_custkey, o_price, o_orderdate, o_state) values (?, ?, ?, ?,0)");
		//p4
		//pay order seckill
		pstmt9 = conn.prepareStatement("select * from orders where o_custkey = ? and o_state = 0");
		pstmt10 = conn.prepareStatement("select o_price from orders where o_orderkey = ?");
		pstmt11 = conn.prepareStatement("update orders set o_state = 1,o_paydate = ? where o_orderkey = ?");
		pstmt12 = conn.prepareStatement("update seckillpay set sa_paycount = sa_paycount + 1 where sa_skpkey = ?");
		//pay order noseckill
		//p9\p10\p11
		//cancel order seckill
		//p9
		pstmt13 = conn.prepareStatement("update orders set o_state = 2 where o_orderkey = ? ");
		pstmt14 = conn.prepareStatement("update seckillplan set sl_skpcount = sl_skpcount - 1 where sl_skpkey = ?");
		//cancel order noseckill
		//p9\p13
		pstmt15 = conn.prepareStatement("select oi_itemkey, oi_count from orderitem where oi_orderkey = ?");
		pstmt16 = conn.prepareStatement("update item set i_count = i_count + ? where i_itemkey = ?");
		//select order
		pstmt17 = conn.prepareStatement("select * from orders where o_custkey = ? limit " + 10);
		pstmt18 = conn.prepareStatement("select * from orders where o_orderkey = ?");
		pstmt19 = conn.prepareStatement("select * from orderitem where oi_orderkey = ?");
	}
	@Override
	public void run()
	{
		Random random = new Random();
		float paycancelratio;
		Operation<MessageWriteProto.MessageWrite> operation = null;
		long[] responseTime = new long[20];
		MessageWriteProto.MessageWrite message = null;
		int secksbmitsucessCount = 0, seckpaysucessCount = 0, seckcancelsucessCount = 0, 
				seckselectsucessCount = 0, seckfailedCount = 0, queuefailCount = 0;
		int nosecksucessCount = 0, noseckpaysucessCount = 0, noseckcancelsucessCount = 0, 
				noseckselectsucessCount = 0, noseckfailedCount = 0;
		SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		long secksubmitTime = 0, seckpayTime = 0, seckcancelTime = 0, seckselectTime = 0;
		long nosecksubmitTime = 0, noseckpayTime = 0, noseckcancelTime = 0, noseckselectTime = 0;
		try
		{
			while(true)
			{
				operation = operationQueue.take();
//				System.out.println("********"+operationQueue.size());
				message = operation.message;
				if(message.getSeckorder()==1)
				{
					if(message.getType()==1)
					{
						//seck sbmit
						int count = data.get(message.getSlskpID());
						//测试直接加压与DB
//						int count = 1;
//						System.out.println("!!!!!!!!!!!"+count);
						if(count>0)
						{
							try{
								pstmt1.setInt(1, message.getSlskpID());
								ResultSet rs = pstmt1.executeQuery();
								Double price = null;
								if(rs.next()) {
									price = rs.getDouble(1);
								} else {
									System.out.println("select error in SubmitOrderThread!");
									continue;
								}
								conn.setAutoCommit(false);
								long transStart = System.nanoTime();
								pstmt2.setInt(1, message.getSlskpID());
								int rows = pstmt2.executeUpdate();
								if(rows == 0) {
									conn.commit();
									seckfailedCount++;
									continue;
								}
								pstmt3.setInt(1, message.getOrderID());
								pstmt3.setInt(2, message.getCostomID());
								pstmt3.setInt(3, message.getSlskpID());
								pstmt3.setDouble(4, price);
								pstmt3.setString(5, dateFormater.format(new Date(new java.util.Date().getTime())));
								//postgres
//								pstmt3.setDate(5, new Date(new java.util.Date().getTime()));
								pstmt3.executeUpdate();
								
								pstmt4.setInt(1, message.getOrderID());
								pstmt4.setInt(2, Integer.valueOf(message.getItemID()));
								pstmt4.setDouble(3, price);
								pstmt4.executeUpdate();
								conn.commit();
								secksubmitTime += (System.nanoTime() - transStart) / 1000;
								secksbmitsucessCount++;
//								data.put(message.getSlskpID(),count-1);
								if(random.nextFloat() < queryRatio)
								{
									otherTask othertask = new otherTask(1, message.getOrderID(), message.getCostomID(),
											message.getSlskpID(), 3);
									otherOperator.addotherOperator(othertask,((int) (timestampcount * Math.random())));
								}
								paycancelratio = random.nextFloat();
								if( paycancelratio < payRatio) {
									otherTask othertask = new otherTask(1, message.getOrderID(), message.getCostomID(),
											message.getSlskpID(), 2);
									otherOperator.addotherOperator(othertask,((int) (timestampcount * Math.random())));
								}
								else if(paycancelratio < (payRatio+cancelRatio)) 
								{
									otherTask othertask = new otherTask(1, message.getOrderID(), message.getCostomID(),
											message.getSlskpID(), 4);
									otherOperator.addotherOperator(othertask,((int) (timestampcount * Math.random())));
								}
							} catch (Exception e) {
								conn.rollback();
								seckfailedCount++;
								e.printStackTrace();
							} finally {
								conn.setAutoCommit(true);
							}
						}
						else
						{
							queuefailCount++;
						}
					}
					else if(message.getType()==2)
					{
						//seck pay
						try{
							pstmt9.setInt(1, message.getCostomID());
							pstmt9.executeQuery();
							pstmt10.setInt(1, message.getOrderID());
							pstmt10.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
							pstmt11.setString(1, dateFormater.format(new Date(new java.util.Date().getTime())));
							//post
//							pstmt11.setDate(1, new Date(new java.util.Date().getTime()));
							pstmt11.setInt(2, message.getOrderID());
							pstmt11.executeUpdate();
							pstmt12.setInt(1, message.getSlskpID());
							pstmt12.executeUpdate();
							conn.commit();
							seckpayTime += (System.nanoTime() - transStart) / 1000;
							seckpaysucessCount++;
						} catch (Exception e) {
							conn.rollback();
							seckfailedCount++;
							e.printStackTrace();
						} finally {
							conn.setAutoCommit(true);
						}
					}
					else if(message.getType()==4)
					{
						//seck cancel
						try{
							pstmt9.setInt(1, message.getCostomID());
							pstmt9.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
							pstmt13.setInt(1, message.getOrderID());
							pstmt13.executeUpdate();
							pstmt14.setInt(1, message.getSlskpID());
							pstmt14.executeUpdate();
							conn.commit();
							seckcancelTime += (System.nanoTime() - transStart) / 1000;
							seckcancelsucessCount++;
						} catch (Exception e) {
							conn.rollback();
							seckfailedCount++;
							e.printStackTrace();
						} finally {
							conn.setAutoCommit(true);
						}
					}
					else if(message.getType()==3)
					{
						try{
							pstmt17.setInt(1, message.getCostomID());
							long transStart = System.nanoTime();
							ResultSet rs = pstmt1.executeQuery();
							seckselectTime += (System.nanoTime() - transStart) / 1000;
							seckselectsucessCount++;
							while(rs.next()) 
							{
								if(random.nextFloat() < 0.5)
								{
									int orderkey = rs.getInt(1);
									pstmt18.setInt(1, orderkey);
									transStart = System.nanoTime();
									pstmt18.executeQuery();
									seckselectTime += (System.nanoTime() - transStart) / 1000;
									seckselectsucessCount++;
									pstmt19.setInt(1, orderkey);
									transStart = System.nanoTime();
									pstmt19.executeQuery();
									seckselectTime += (System.nanoTime() - transStart) / 1000;
									seckselectsucessCount++;
								}
							}
						} catch (Exception e) {
							conn.rollback();
							seckfailedCount++;
							e.printStackTrace();
						}
					}
				}
				else
				{
					if(message.getType()==1)
					{
						try{
							String[] items = message.getItemID().split(",");
							double[] prices = new double[items.length];
							double allprice = 0;
							ResultSet rs = null;
							boolean flag = true;
							for(int i = 0; i < items.length; i++)
							{
								pstmt5.setInt(1, Integer.valueOf((items[i].trim())));
								rs = pstmt5.executeQuery();
								if(rs.next()) {
									prices[i] = rs.getDouble(1);
									allprice += prices[i];
								}
								else {
									System.out.println("select error in NOSubmitOrder!");
									continue;
								}
							}
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
							for(int i = 0; i < items.length; i++)
							{
								pstmt6.setInt(1, Integer.valueOf((items[i].trim())));
								int rows = pstmt6.executeUpdate();
								if(rows == 0) {
									conn.commit();
									seckfailedCount++;
									flag = false;
									break;
								}
							}
							if(!flag)
								continue;
							pstmt7.setInt(1, message.getOrderID());
							pstmt7.setInt(2, message.getCostomID());
							pstmt7.setDouble(3, allprice);
							pstmt7.setString(4, dateFormater.format(new Date(new java.util.Date().getTime())));
//							pstmt7.setDate(4, new Date(new java.util.Date().getTime()));
							pstmt7.executeUpdate();
							for(int i = 0; i < items.length; i++)
							{
								pstmt4.setInt(1, message.getOrderID());
								pstmt4.setInt(2, Integer.valueOf(items[i].trim()));
								pstmt4.setDouble(3, prices[i]);
								pstmt4.executeUpdate();
							}
							conn.commit();
							nosecksubmitTime += (System.nanoTime() - transStart) / 1000;
							nosecksucessCount++;
							if(random.nextFloat() < queryRatio)
							{
								otherTask othertask = new otherTask(2,message.getOrderID(),message.getCostomID(),3);
								otherOperator.addotherOperator(othertask,((int) (timestampcount * Math.random())));
							}
							paycancelratio = random.nextFloat();
							if( paycancelratio < payRatio) {
								otherTask othertask = new otherTask(2,message.getOrderID(),message.getCostomID(),2);
								otherOperator.addotherOperator(othertask,((int) (timestampcount * Math.random())));
							}
							else if(paycancelratio < (payRatio+cancelRatio)) 
							{
								otherTask othertask = new otherTask(2,message.getOrderID(),message.getCostomID(),4);
								otherOperator.addotherOperator(othertask,((int) (timestampcount * Math.random())));
							}
						} catch (Exception e) {
							conn.rollback();
							noseckfailedCount++;
							e.printStackTrace();
						} finally {
							conn.setAutoCommit(true);
						}
					}
					else if(message.getType()==2)
					{
						try{
							pstmt9.setInt(1, message.getCostomID());
							pstmt9.executeQuery();
							pstmt10.setInt(1, message.getOrderID());
							pstmt10.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
							pstmt11.setString(1, dateFormater.format(new Date(new java.util.Date().getTime())));
//							pstmt11.setDate(1, new Date(new java.util.Date().getTime()));
							pstmt11.setInt(2, message.getOrderID());
							pstmt11.executeUpdate();
							conn.commit();
							noseckpayTime += (System.nanoTime() - transStart) / 1000;
							noseckpaysucessCount++;
						} catch (Exception e) {
							conn.rollback();
							noseckfailedCount++;
							e.printStackTrace();
						} finally {
							conn.setAutoCommit(true);
						}
					}
					else if(message.getType()==3)
					{
						try{
							pstmt17.setInt(1, message.getCostomID());
							long transStart = System.nanoTime();
							ResultSet rs = pstmt1.executeQuery();
							noseckselectTime += (System.nanoTime() - transStart) / 1000;
							noseckselectsucessCount++;
							while(rs.next()) 
							{
								if(random.nextFloat() < 0.5)
								{
									int orderkey = rs.getInt(1);
									pstmt18.setInt(1, orderkey);
									transStart = System.nanoTime();
									pstmt18.executeQuery();
									noseckselectTime += (System.nanoTime() - transStart) / 1000;
									noseckselectsucessCount++;
									pstmt19.setInt(1, orderkey);
									transStart = System.nanoTime();
									pstmt19.executeQuery();
									noseckselectTime += (System.nanoTime() - transStart) / 1000;
									noseckselectsucessCount++;
								}
							}
						} catch (Exception e) {
							conn.rollback();
							noseckfailedCount++;
							e.printStackTrace();
						}
					}
					else if(message.getType()==4)
					{
						try{
							pstmt9.setInt(1, message.getCostomID());
							pstmt9.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
							pstmt13.setInt(1, message.getOrderID());
							pstmt13.executeUpdate();
							pstmt15.setInt(1, message.getOrderID());
							ResultSet rs = pstmt15.executeQuery();
							while(rs.next()) {
								int itemkey = rs.getInt(1);
								int count = rs.getInt(2);
								pstmt16.setInt(1, count);
								pstmt16.setInt(2, itemkey);
								pstmt16.executeUpdate();
							}
							conn.commit();
							noseckcancelTime += (System.nanoTime() - transStart) / 1000;
							noseckcancelsucessCount++;
						} catch (Exception e) {
							conn.rollback();
							noseckfailedCount++;
							e.printStackTrace();
						} finally {
							conn.setAutoCommit(true);
						}
					}
				}
//				System.out.println("~~~~~~~~~~~~~"+secksbmitsucessCount);
				responseTime [0] = secksbmitsucessCount;
				responseTime [1] = secksubmitTime;
				responseTime [2] = seckpaysucessCount;
				responseTime [3] = seckpayTime;
				responseTime [4] = seckcancelsucessCount;
				responseTime [5] = seckcancelTime;
				responseTime [6] = seckselectsucessCount;
				responseTime [7] = seckselectTime;		
				responseTime [8] = nosecksucessCount;
				responseTime [9] = nosecksubmitTime;
				responseTime [10] = noseckpaysucessCount;
				responseTime [11] = noseckpayTime;
				responseTime [12] = noseckcancelsucessCount;
				responseTime [13] = noseckcancelTime;
				responseTime [14] = noseckselectsucessCount;
				responseTime [15] = noseckselectTime;
				responseTime [16] = seckfailedCount;
				responseTime [17] = queuefailCount;
				responseTime [18] = noseckfailedCount;			
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
				if(pstmt6 != null)	pstmt6.close();
				if(pstmt7 != null)	pstmt7.close();
				if(pstmt9 != null)	pstmt9.close();
				if(pstmt10 != null)	pstmt10.close();
				if(pstmt11 != null)	pstmt11.close();
				if(pstmt12 != null)	pstmt12.close();
				if(pstmt13 != null)	pstmt13.close();
				if(pstmt14 != null)	pstmt14.close();
				if(pstmt15 != null)	pstmt15.close();
				if(pstmt16 != null)	pstmt16.close();
				if(pstmt17 != null)	pstmt17.close();
				if(pstmt18 != null)	pstmt18.close();
				if(pstmt19 != null)	pstmt19.close();
				if(pstmt20 != null)	pstmt20.close();
				if(conn != null)	conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}	
	}
	
}
