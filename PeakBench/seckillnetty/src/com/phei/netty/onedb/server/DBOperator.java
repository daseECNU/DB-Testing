package com.phei.netty.onedb.server;

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

import com.phei.netty.onedb.client.MessageProto;

import util.DBConnector;
import util.Operation;
import util.RanDataGene;
import util.SeckillItem;
import util.otherTask;

public class DBOperator
{
	private int threadNum;
	private int queueSize;
	private DBConnector dbConnector = null;
	private ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>> operationQueues = null;
	private CopyOnWriteArrayList<long[]> readresponseTimeList = new CopyOnWriteArrayList<long[]>();
	private CopyOnWriteArrayList<long[]> writeresponseTimeList = new CopyOnWriteArrayList<long[]>();
	private ArrayList<HashMap<Integer, Integer>> dbDataList = null;
	private ArrayList<SeckillItem> dbData = null;
	private HashMap<Integer, ArrayList<Integer>> hashselect = new HashMap<Integer, ArrayList<Integer>>();
	private int basenum = 0;
	public long gathertime = 0;
	public int readtimestamp = 0, writetimestamp = 0, readwriteinterval = 0;
	public boolean hashflag = true;
	public OtherOperator otherOperator = null;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	public DBOperator(int threadNum, int queueSize, DBConnector dbConnector,
			ArrayList<SeckillItem> dbData, int basenum, long gathertime, 
			int readtimestamp, int writetimestamp, int readwriteinterval,
			float payRatio, float cancelRatio,float queryRatio)
	{
		super();
		this.threadNum = threadNum;
		this.queueSize = queueSize;
		this.dbConnector = dbConnector;
		this.dbData = dbData;
		this.basenum = basenum;
		this.gathertime = gathertime;
		this.readtimestamp = readtimestamp;
		this.writetimestamp = writetimestamp;
		this.readwriteinterval = readwriteinterval;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		init();
	}
	private void init()
	{ 
		operationQueues = new ArrayList<ArrayBlockingQueue<Operation<MessageProto.Message>>>(threadNum);
		dbDataList = new ArrayList<HashMap<Integer, Integer>>(threadNum);
		for (int i = 0; i < threadNum; i++)
			operationQueues.add(new ArrayBlockingQueue<Operation<MessageProto.Message>>(queueSize, true));
		for(int i = 0; i < threadNum; i++) {
			readresponseTimeList.add(new long[0]);
			writeresponseTimeList.add(new long[0]);
		}
		for(int i = 0; i < threadNum; i++){
			dbDataList.add(new HashMap<Integer, Integer>());
		}
		createHashmap();
		if(writetimestamp!=0)
		{
			otherOperator = new OtherOperator(writetimestamp, readwriteinterval);
			new Thread(otherOperator).start();
		}
		
		for (int i = 0; i < threadNum; i++)
		{
			try{
				new Thread(new DBOperatorThread(operationQueues.get(i), readresponseTimeList, 
						writeresponseTimeList, dbConnector.getConnection(), i, otherOperator, writetimestamp,
						payRatio, cancelRatio, queryRatio, dbDataList.get(i))).start();
			}catch (FileNotFoundException e){
				e.printStackTrace();
			}
		}
		new Timer().schedule(new TimerTask() { 
            MessageReturnProto.MessageReturn.Builder workMessage = 
            MessageReturnProto.MessageReturn.newBuilder();
            long secktime = 0, seckcount = 0, seckallcount = 0, secklastallcount = 0;
			long nosecktime = 0, noseckcount = 0, noseckallcount = 0, nosecklastallcount = 0;
			long seckallfailcount = 0, secklastallfailcount = 0,
				noseckallfailcount = 0, nosecklastallfailcount = 0,
				seckallfailqueuecount = 0, secklastallfailqueuecount = 0;
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
        				if(readresponseTimeList.get(j).length!=0)
        				{
        					time += readresponseTimeList.get(j)[i];
        					count += readresponseTimeList.get(j)[i + 1];
        				}
        			}
        			
    				if((i / 2 + 1)==1)
    				{	
    					if(count!=0)
    						workMessage.setRtime1(((float)time / count));
    					else
    						workMessage.setRtime1(0);
    				}
        			else if((i / 2 + 1)==2)
        			{	
        				if(count!=0)
    						workMessage.setRtime2(((float)time / count));
    					else
    						workMessage.setRtime2(0);
        			}
        			else if((i / 2 + 1)==3)
        			{
        				if(count!=0)
    						workMessage.setRtime3(((float)time / count));
    					else
    						workMessage.setRtime3(0);
        			}
        			else if((i / 2 + 1)==4)
        			{
        				if(count!=0)
    						workMessage.setRtime4(((float)time / count));
    					else
    						workMessage.setRtime4(0);
        			}
        			else if((i / 2 + 1)==5)
        			{
        				if(count!=0)
    						workMessage.setRtime5(((float)time / count));
    					else
    						workMessage.setRtime5(0);
        			}
//    				System.out.println("!!!*******"+count+"*****");
        			allcount += count;
        		}
                System.out.println("*******"+(allcount - lastallcount)+"*****");
                workMessage.setQps((float)(allcount - lastallcount)*1000/gathertime);
                
                for(int i = 0; i < 8; i+=2) 
                {
        			secktime = 0; seckcount = 0;
        			for(int j = 0; j < threadNum ; j++) 
        			{
        				if(writeresponseTimeList.get(j).length!=0)
        				{
        					seckcount += writeresponseTimeList.get(j)[i];
        					secktime += writeresponseTimeList.get(j)[i + 1];
        				}
        			}        			
    				if((i / 2 + 1)==1)
    				{	
    					if(seckcount!=0)
    						workMessage.setWtime1(((float)secktime / seckcount));
    					else
    						workMessage.setWtime1(0);
    				}
        			else if((i / 2 + 1)==2)
        			{	
        				if(seckcount!=0)
    						workMessage.setWtime2(((float)secktime / seckcount));
    					else
    						workMessage.setWtime2(0);
        			}
        			else if((i / 2 + 1)==3)
        			{
        				if(seckcount!=0)
    						workMessage.setWtime3(((float)secktime / seckcount));
    					else
    						workMessage.setWtime3(0);
        			}
        			else if((i / 2 + 1)==4)
        			{
        				if(seckcount!=0)
    						workMessage.setWtime4(((float)secktime / seckcount));
    					else
    						workMessage.setWtime4(0);
        			}
    				seckallcount += seckcount;
        		}
                for(int i = 8; i < 16; i+=2) 
                {
        			nosecktime = 0; noseckcount = 0;
        			for(int j = 0; j < threadNum ; j++) 
        			{
        				if(writeresponseTimeList.get(j).length!=0)
        				{
        					noseckcount += writeresponseTimeList.get(j)[i];
        					nosecktime += writeresponseTimeList.get(j)[i + 1];
        				}
        			}        			
    				if((i / 2 + 1)==5)
    				{	
    					if(noseckcount!=0)
    						workMessage.setWtime5(((float)nosecktime / noseckcount));
    					else
    						workMessage.setWtime5(0);
    				}
        			else if((i / 2 + 1)==6)
        			{	
        				if(noseckcount!=0)
    						workMessage.setWtime6(((float)nosecktime / noseckcount));
    					else
    						workMessage.setWtime6(0);
        			}
        			else if((i / 2 + 1)==7)
        			{
        				if(noseckcount!=0)
    						workMessage.setWtime7(((float)nosecktime / noseckcount));
    					else
    						workMessage.setWtime7(0);
        			}
        			else if((i / 2 + 1)==8)
        			{
        				if(noseckcount!=0)
    						workMessage.setWtime8(((float)nosecktime / noseckcount));
    					else
    						workMessage.setWtime8(0);
        			}
    				noseckallcount += noseckcount;
        		}
                for(int j = 0; j < threadNum ; j++) 
    			{
    				if(writeresponseTimeList.get(j).length!=0)
    				{
    					seckallfailcount += writeresponseTimeList.get(j)[16];
    				}
    			} 
                for(int j = 0; j < threadNum ; j++) 
    			{
    				if(writeresponseTimeList.get(j).length!=0)
    				{
    					seckallfailqueuecount += writeresponseTimeList.get(j)[17];
    				}
    			} 
                for(int j = 0; j < threadNum ; j++) 
    			{
    				if(writeresponseTimeList.get(j).length!=0)
    				{
    					noseckallfailcount += writeresponseTimeList.get(j)[18];
    				}
    			}
                System.out.println("!!!!!!!!!!!"+(seckallcount+noseckallcount-secklastallcount-nosecklastallcount)+"*****");
                workMessage.setSecksucesscount(seckallcount - secklastallcount);
                workMessage.setNosecksucesscount(noseckallcount - nosecklastallcount);
                workMessage.setSeckfailcount(seckallfailcount - secklastallfailcount);
                workMessage.setNoseckfailcount(noseckallfailcount - nosecklastallfailcount);
                workMessage.setQueuefailcount(seckallfailqueuecount - secklastallfailqueuecount);
                workMessage.setTps((float)(seckallcount+noseckallcount-secklastallcount-nosecklastallcount)*1000/gathertime);
                
                ReturnNettyClient.routeWorkOrder(workMessage.build());

                if(timestamp > readwriteinterval + writetimestamp && allcount == lastallcount 
                		 && seckallcount == secklastallcount && noseckallcount == nosecklastallcount
                    	 && seckallfailcount == secklastallfailcount && noseckallfailcount == nosecklastallfailcount
                    	 && seckallfailqueuecount == secklastallfailqueuecount)
                {
                	cancel();
                }
                lastallcount = allcount;
                allcount = 0;
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
				dbDataList.get((item.skpkey/basenum)%threadNum).put(item.skpkey, item.plancount);
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
	public void addOperation(Operation<MessageProto.Message> operation)
	{
		try
		{
			if(operation.message.getMessageWrite().getSeckorder() == 1)
			{
				if(hashflag)
				{
					operationQueues.get((operation.message.getMessageWrite().getSlskpID()/basenum)%threadNum).put(operation);
				}
				else
				{
					operationQueues.get(getIndex(operation.message.getMessageWrite().getSlskpID())).put(operation);
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
	private ArrayBlockingQueue<Operation<MessageProto.Message>> operationQueue = null;
	private HashMap<Integer, Integer> data = null;
	private Connection conn = null;
	private PreparedStatement rpstmt1 = null, rpstmt2 = null, rpstmt3 = null, rpstmt4 = null, rpstmt5 = null;
	private PreparedStatement wpstmt1 = null, wpstmt2 = null, wpstmt3 = null, wpstmt4 = null, wpstmt5 = null,
			wpstmt6 = null, wpstmt7 = null, wpstmt9 = null, wpstmt10 = null, 
			wpstmt11 = null, wpstmt12 = null, wpstmt13 = null, wpstmt14 = null, wpstmt15 = null,
			wpstmt16 = null, wpstmt17 = null, wpstmt18 = null, wpstmt19 = null, wpstmt20 = null;
	private Statement pst = null;
	private CopyOnWriteArrayList<long[]> readresponseTimeList = null;
	private CopyOnWriteArrayList<long[]> writeresponseTimeList = null;
	private int thread_th = 0;
	public OtherOperator otherOperator = null;
	public int timestampcount = 0;
	private float payRatio;
	private float cancelRatio;
	private float queryRatio;
	long[] readresponseTime = new long[10];
	int count1 = 0, count2 = 0, count3 = 0, count4 = 0, count5 = 0;
	long time1 = 0, time2 = 0, time3 = 0, time4 = 0, time5 = 0;
	Random random = new Random();
	float paycancelratio;
	long[] writeresponseTime = new long[20];
	int secksbmitsucessCount = 0, seckpaysucessCount = 0, seckcancelsucessCount = 0, 
			seckselectsucessCount = 0, seckfailedCount = 0, queuefailCount = 0;
	int nosecksucessCount = 0, noseckpaysucessCount = 0, noseckcancelsucessCount = 0, 
			noseckselectsucessCount = 0, noseckfailedCount = 0;
	SimpleDateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	long secksubmitTime = 0, seckpayTime = 0, seckcancelTime = 0, seckselectTime = 0;
	long nosecksubmitTime = 0, noseckpayTime = 0, noseckcancelTime = 0, noseckselectTime = 0;
	
	public DBOperatorThread(ArrayBlockingQueue<Operation<MessageProto.Message>> operationQueue, 
			CopyOnWriteArrayList<long[]> readresponseTimeList, CopyOnWriteArrayList<long[]> writeresponseTimeList,
			Connection conn, int thread_th, OtherOperator otherOperator, int timestampcount,
			float payRatio, float cancelRatio,float queryRatio, HashMap<Integer, Integer> data)
	{
		super();
		this.operationQueue = operationQueue;
		this.readresponseTimeList = readresponseTimeList;
		this.writeresponseTimeList = writeresponseTimeList;
		this.conn = conn;
		this.thread_th = thread_th;
		this.otherOperator = otherOperator;
		this.timestampcount = timestampcount;
		this.payRatio = payRatio;
		this.cancelRatio = cancelRatio;
		this.queryRatio = queryRatio;
		this.data = data;
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
//	    pst.execute("set @@session.ob_query_timeout=9000000000;");
		rpstmt1 = conn.prepareStatement("select * from item where i_itemkey = ?");
		rpstmt2 = conn.prepareStatement("select * from item where i_type = ? limit ?");
		rpstmt3 = conn.prepareStatement("select * from item where i_type = ? and i_name like ? limit ?");
		rpstmt4 = conn.prepareStatement("select * from item where i_type = ? and i_price between ? and ? limit ?" );
		rpstmt5 = conn.prepareStatement("select * from item where i_suppkey = ? limit ?" );
		 //submit order seckill
	    wpstmt1 = conn.prepareStatement("select sl_price from seckillplan where sl_skpkey = ?");
		wpstmt2 = conn.prepareStatement("update seckillplan set sl_skpcount = sl_skpcount + 1 where sl_skpkey = ? and sl_skpcount < sl_plancount");
		wpstmt3 = conn.prepareStatement("insert into orders (o_orderkey, o_custkey, o_skpkey, o_price, o_orderdate, o_state) values (?, ?, ?, ?, ?, 0)");
		wpstmt4 = conn.prepareStatement("insert into orderitem (oi_orderkey, oi_itemkey, oi_count, oi_price) values (?, ?, 1, ?)");
		//submit order noseckill
		wpstmt5 = conn.prepareStatement("select i_price from item where i_itemkey = ?");
		wpstmt6 = conn.prepareStatement("update item set i_count = i_count - 1 where i_itemkey = ? and i_count >= 1");
		wpstmt7 = conn.prepareStatement("insert into orders (o_orderkey, o_custkey, o_price, o_orderdate, o_state) values (?, ?, ?, ?,0)");
		//p4
		//pay order seckill
		wpstmt9 = conn.prepareStatement("select * from orders where o_custkey = ? and o_state = 0");
		wpstmt10 = conn.prepareStatement("select o_price from orders where o_orderkey = ?");
		wpstmt11 = conn.prepareStatement("update orders set o_state = 1,o_paydate = ? where o_orderkey = ?");
		wpstmt12 = conn.prepareStatement("update seckillpay set sa_paycount = sa_paycount + 1 where sa_skpkey = ?");
		//pay order noseckill
		//p9\p10\p11
		//cancel order seckill
		//p9
		wpstmt13 = conn.prepareStatement("update orders set o_state = 2 where o_orderkey = ? ");
		wpstmt14 = conn.prepareStatement("update seckillplan set sl_skpcount = sl_skpcount - 1 where sl_skpkey = ?");
		//cancel order noseckill
		//p9\p13
		wpstmt15 = conn.prepareStatement("select oi_itemkey, oi_count from orderitem where oi_orderkey = ?");
		wpstmt16 = conn.prepareStatement("update item set i_count = i_count + ? where i_itemkey = ?");
		//select order
		wpstmt17 = conn.prepareStatement("select * from orders where o_custkey = ? limit " + 10);
		wpstmt18 = conn.prepareStatement("select * from orders where o_orderkey = ?");
		wpstmt19 = conn.prepareStatement("select * from orderitem where oi_orderkey = ?");
	}
	public void run()
	{	
		while (true)
		{
			Operation<MessageProto.Message> operation = null;
			MessageProto.Message message = null;
			try{
				operation = operationQueue.take();
			}catch (InterruptedException e){
				e.printStackTrace();
			}
			message = operation.message;
//				System.out.println("*******"+message.getItemID());
			if(message.getType()==1)
			{
//					System.out.println("！！！！！！！！*******"+"read......");
				ReadOperator(message.getMessageRead());
			}
			else if(message.getType()==2)
			{
//				System.out.println("！！！！！！！！*******"+"write......");
				WriteOperator(message.getMessageWrite());
			}
		}
		
	}
	public void ReadOperator(MessageProto.MessageRead message)
	{
		try
		{
			if (message.getType() == 1)
			{
//				System.out.println("！！！！！！！！*******"+message.getItemID()+"!!!!!!!!!!****"+count1);
				rpstmt1.setInt(1, message.getItemID());
				long start = System.nanoTime();
				rpstmt1.executeQuery();
				time1 += (System.nanoTime() - start) / 1000;
				count1++;
			}
			else if (message.getType() == 2)
			{
//				System.out.println("！！！！！！！！*******"+message.getTypeID());
				rpstmt2.setInt(1, message.getTypeID());
				rpstmt2.setInt(2, message.getLimite());
				long start = System.nanoTime();
				rpstmt2.executeQuery();
				time2 += (System.nanoTime() - start) / 1000;
				count2++;
			}
			else if (message.getType() == 3)
			{
				rpstmt3.setInt(1, message.getTypeID());
				rpstmt3.setString(2, RanDataGene.getChar() + RanDataGene.getChar() + "%");
				rpstmt3.setInt(3, message.getLimite());
				long start = System.nanoTime();
				rpstmt3.executeQuery();
				time3 += (System.nanoTime() - start) / 1000;
				count3++;
			}
			else if (message.getType() == 4)
			{
				float price1 = RanDataGene.getInteger(1, 500) - random.nextFloat();
				float price2 = price1 + RanDataGene.getInteger(1, 500);
				rpstmt4.setInt(1, message.getTypeID());
				rpstmt4.setFloat(2, price1);
				rpstmt4.setFloat(3, price2);
				rpstmt4.setInt(4, message.getLimite());
				long start = System.nanoTime();
				rpstmt4.executeQuery();
				time4 += (System.nanoTime() - start) / 1000;
				count4++;
			}
			else if (message.getType() == 5)
			{
				rpstmt5.setInt(1, message.getSuppID());
				rpstmt5.setInt(2, message.getLimite());
				long start = System.nanoTime();
				rpstmt5.executeQuery();
				time5 += (System.nanoTime() - start) / 1000;
				count5++;
			}
			readresponseTime [0] = time1;
			readresponseTime [1] = count1;
			readresponseTime [2] = time2;
			readresponseTime [3] = count2;
			readresponseTime [4] = time3;
			readresponseTime [5] = count3;
			readresponseTime [6] = time4;
			readresponseTime [7] = count4;
			readresponseTime [8] = time5;
			readresponseTime [9] = count5;
			readresponseTimeList.set(thread_th ,readresponseTime);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			try {
//				if(rpstmt1 != null)	rpstmt1.close();
//				if(rpstmt2 != null)	rpstmt2.close();
//				if(rpstmt3 != null)	rpstmt3.close();
//				if(rpstmt4 != null)	rpstmt4.close();
//				if(rpstmt5 != null)	rpstmt5.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void WriteOperator(MessageProto.MessageWrite message)
	{
		try
		{
//				System.out.println("********"+operationQueue.size());
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
								wpstmt1.setInt(1, message.getSlskpID());
								ResultSet rs = wpstmt1.executeQuery();
								Double price = null;
								if(rs.next()) {
									price = rs.getDouble(1);
								} else {
									System.out.println("select error in SubmitOrderThread!");
									return;
								}
								conn.setAutoCommit(false);
								long transStart = System.nanoTime();
								wpstmt2.setInt(1, message.getSlskpID());
								int rows = wpstmt2.executeUpdate();
								if(rows == 0) {
									conn.commit();
									seckfailedCount++;
									return;
								}
								wpstmt3.setInt(1, message.getOrderID());
								wpstmt3.setInt(2, message.getCostomID());
								wpstmt3.setInt(3, message.getSlskpID());
								wpstmt3.setDouble(4, price);
//								wpstmt3.setString(5, dateFormater.format(new Date(new java.util.Date().getTime())));
								//postgres
								wpstmt3.setDate(5, new Date(new java.util.Date().getTime()));
								wpstmt3.executeUpdate();
								
								wpstmt4.setInt(1, message.getOrderID());
								wpstmt4.setInt(2, Integer.valueOf(message.getItemID()));
								wpstmt4.setDouble(3, price);
								wpstmt4.executeUpdate();
								conn.commit();
								secksubmitTime += (System.nanoTime() - transStart) / 1000;
								secksbmitsucessCount++;
								//记录data的
								data.put(message.getSlskpID(),count-1);
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
							wpstmt9.setInt(1, message.getCostomID());
							wpstmt9.executeQuery();
							wpstmt10.setInt(1, message.getOrderID());
							wpstmt10.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
//							wpstmt11.setString(1, dateFormater.format(new Date(new java.util.Date().getTime())));
							//post
							wpstmt11.setDate(1, new Date(new java.util.Date().getTime()));
							wpstmt11.setInt(2, message.getOrderID());
							wpstmt11.executeUpdate();
							wpstmt12.setInt(1, message.getSlskpID());
							wpstmt12.executeUpdate();
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
							wpstmt9.setInt(1, message.getCostomID());
							wpstmt9.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
							wpstmt13.setInt(1, message.getOrderID());
							wpstmt13.executeUpdate();
							wpstmt14.setInt(1, message.getSlskpID());
							wpstmt14.executeUpdate();
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
							wpstmt17.setInt(1, message.getCostomID());
							long transStart = System.nanoTime();
							ResultSet rs = wpstmt1.executeQuery();
							seckselectTime += (System.nanoTime() - transStart) / 1000;
							seckselectsucessCount++;
							while(rs.next()) 
							{
								if(random.nextFloat() < 0.5)
								{
									int orderkey = rs.getInt(1);
									wpstmt18.setInt(1, orderkey);
									transStart = System.nanoTime();
									wpstmt18.executeQuery();
									seckselectTime += (System.nanoTime() - transStart) / 1000;
									seckselectsucessCount++;
									wpstmt19.setInt(1, orderkey);
									transStart = System.nanoTime();
									wpstmt19.executeQuery();
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
								wpstmt5.setInt(1, Integer.valueOf((items[i].trim())));
								rs = wpstmt5.executeQuery();
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
								wpstmt6.setInt(1, Integer.valueOf((items[i].trim())));
								int rows = wpstmt6.executeUpdate();
								if(rows == 0) {
									conn.commit();
									seckfailedCount++;
									flag = false;
									break;
								}
							}
							if(!flag)
								return;
							wpstmt7.setInt(1, message.getOrderID());
							wpstmt7.setInt(2, message.getCostomID());
							wpstmt7.setDouble(3, allprice);
//							wpstmt7.setString(4, dateFormater.format(new Date(new java.util.Date().getTime())));
							//postgres
							wpstmt7.setDate(4, new Date(new java.util.Date().getTime()));
							wpstmt7.executeUpdate();
							for(int i = 0; i < items.length; i++)
							{
								wpstmt4.setInt(1, message.getOrderID());
								wpstmt4.setInt(2, Integer.valueOf(items[i].trim()));
								wpstmt4.setDouble(3, prices[i]);
								wpstmt4.executeUpdate();
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
							wpstmt9.setInt(1, message.getCostomID());
							wpstmt9.executeQuery();
							wpstmt10.setInt(1, message.getOrderID());
							wpstmt10.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
//							wpstmt11.setString(1, dateFormater.format(new Date(new java.util.Date().getTime())));
							//postgres
							wpstmt11.setDate(1, new Date(new java.util.Date().getTime()));
							wpstmt11.setInt(2, message.getOrderID());
							wpstmt11.executeUpdate();
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
							wpstmt17.setInt(1, message.getCostomID());
							long transStart = System.nanoTime();
							ResultSet rs = wpstmt1.executeQuery();
							noseckselectTime += (System.nanoTime() - transStart) / 1000;
							noseckselectsucessCount++;
							while(rs.next()) 
							{
								if(random.nextFloat() < 0.5)
								{
									int orderkey = rs.getInt(1);
									wpstmt18.setInt(1, orderkey);
									transStart = System.nanoTime();
									wpstmt18.executeQuery();
									noseckselectTime += (System.nanoTime() - transStart) / 1000;
									noseckselectsucessCount++;
									wpstmt19.setInt(1, orderkey);
									transStart = System.nanoTime();
									wpstmt19.executeQuery();
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
							wpstmt9.setInt(1, message.getCostomID());
							wpstmt9.executeQuery();
							conn.setAutoCommit(false);
							long transStart = System.nanoTime();
							wpstmt13.setInt(1, message.getOrderID());
							wpstmt13.executeUpdate();
							wpstmt15.setInt(1, message.getOrderID());
							ResultSet rs = wpstmt15.executeQuery();
							while(rs.next()) {
								int itemkey = rs.getInt(1);
								int count = rs.getInt(2);
								wpstmt16.setInt(1, count);
								wpstmt16.setInt(2, itemkey);
								wpstmt16.executeUpdate();
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
				writeresponseTime [0] = secksbmitsucessCount;
				writeresponseTime [1] = secksubmitTime;
				writeresponseTime [2] = seckpaysucessCount;
				writeresponseTime [3] = seckpayTime;
				writeresponseTime [4] = seckcancelsucessCount;
				writeresponseTime [5] = seckcancelTime;
				writeresponseTime [6] = seckselectsucessCount;
				writeresponseTime [7] = seckselectTime;		
				writeresponseTime [8] = nosecksucessCount;
				writeresponseTime [9] = nosecksubmitTime;
				writeresponseTime [10] = noseckpaysucessCount;
				writeresponseTime [11] = noseckpayTime;
				writeresponseTime [12] = noseckcancelsucessCount;
				writeresponseTime [13] = noseckcancelTime;
				writeresponseTime [14] = noseckselectsucessCount;
				writeresponseTime [15] = noseckselectTime;
				writeresponseTime [16] = seckfailedCount;
				writeresponseTime [17] = queuefailCount;
				writeresponseTime [18] = noseckfailedCount;			
				writeresponseTimeList.set(thread_th ,writeresponseTime);
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			try {
//				if(wpstmt1 != null)	wpstmt1.close();
//				if(wpstmt2 != null)	wpstmt2.close();
//				if(wpstmt3 != null)	wpstmt3.close();
//				if(wpstmt4 != null)	wpstmt4.close();
//				if(wpstmt5 != null)	wpstmt5.close();
//				if(wpstmt6 != null)	wpstmt6.close();
//				if(wpstmt7 != null)	wpstmt7.close();
//				if(wpstmt9 != null)	wpstmt9.close();
//				if(wpstmt10 != null)	wpstmt10.close();
//				if(wpstmt11 != null)	wpstmt11.close();
//				if(wpstmt12 != null)	wpstmt12.close();
//				if(wpstmt13 != null)	wpstmt13.close();
//				if(wpstmt14 != null)	wpstmt14.close();
//				if(wpstmt15 != null)	wpstmt15.close();
//				if(wpstmt16 != null)	wpstmt16.close();
//				if(wpstmt17 != null)	wpstmt17.close();
//				if(wpstmt18 != null)	wpstmt18.close();
//				if(wpstmt19 != null)	wpstmt19.close();
//				if(wpstmt20 != null)	wpstmt20.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
