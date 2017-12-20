package com.phei.netty.onedb.server;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import util.DBConnector;
import util.SeckillItem;
import util.ServerConfig;

public class ProceseNode
{
	private static ArrayList<DBConnector> dbConnectors = null;
	private static ArrayList<DBOperator> dbOperators = null;
	private static int serverNum;
	private static ArrayList<ArrayList<SeckillItem>> dbDatas = null;
	
	public static void init(int Num, String ipFile, String seckillplanFile, 
			int threadNum, int queueSize, int gathertime, int readtime, int writetime,
			int readwriteinterval, float payRatio, float cancelRatio,float queryRatio)
	{
		serverNum = Num;
		BufferedReader ipbr = null;
		try {
			ipbr = new BufferedReader(new InputStreamReader(new FileInputStream(ipFile)));
			String[] ip = null;
			String ipLine = null;
			dbConnectors = new ArrayList<DBConnector>();
			while((ipLine = ipbr.readLine())!=null)
			{
				ip = ipLine.split(",");
				dbConnectors.add(new DBConnector(ip[0], ip[1], ip[2], ip[3], ip[4], ip[5], ip[6]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(ipbr != null)	ipbr.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		BufferedReader br = null;
		dbDatas = new ArrayList<ArrayList<SeckillItem>>();
		for(int i = 0; i < dbConnectors.size(); i ++)
		{
			dbDatas.add(new ArrayList<SeckillItem>());
		}
		try
		{
			br = new BufferedReader(new InputStreamReader(new FileInputStream(seckillplanFile), "utf-8"));
			String inputLine = null;
			String[] arr;
			while((inputLine = br.readLine()) != null) {
				arr = inputLine.split(",");
				dbDatas.get((Integer.parseInt(arr[0])/serverNum)%dbConnectors.size())
				.add(new SeckillItem(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Integer.parseInt(arr[5])));
			}
		}catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null)	br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		dbOperators = new ArrayList<DBOperator>();
		for(int i=0;i<dbConnectors.size();i++)
		{
			dbOperators.add(new DBOperator(threadNum, queueSize, dbConnectors.get(i), dbDatas.get(i),
			(serverNum*dbConnectors.size()), gathertime, readtime, writetime, readwriteinterval,
			payRatio, cancelRatio, queryRatio));
		}
	}
	public static DBOperator getDbOperator(int slskpkey)
	{
		return dbOperators.get((slskpkey/serverNum)%dbConnectors.size());
	}
	
	public static DBOperator getDbOperator()
	{
		Random random = new Random();
		return dbOperators.get(random.nextInt(dbConnectors.size()));
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		ServerConfig server = new ServerConfig("/Server.properties");
		server.loadProperties();
		ProceseNode.init(server.getInt("serverNum",1), server.getString("ipfile"), 
				server.getString("seckillplanfile"), server.getInt("threadNum",100),
				server.getInt("queueSize",100), server.getInt("gathertime",1000),
				server.getInt("readtime",120), server.getInt("writetime",120),
				server.getInt("readwriteinterval",120), server.getFloat("payRatio", 0),
				server.getFloat("cancelRatio", 0), server.getFloat("queryRatio", 0));
		//		//开启服务端监听
		new Thread(new NettyServer(server.getString("serverport"))).start();
//				
//		//开一个返回统计信息的netty client
		new Thread(new ReturnNettyClient(server.getString("clienthost"), server.getString("clientport"))).start();
		
	}
}
