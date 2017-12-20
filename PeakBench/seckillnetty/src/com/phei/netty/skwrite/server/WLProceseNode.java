package com.phei.netty.skwrite.server;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import util.DBConnector;
import util.SeckillItem;
import util.ServerConfig;

public class WLProceseNode
{
	private static ArrayList<DBConnector> dbConnectors = null;
	private static ArrayList<WLDBOperator> dbOperators = null;
	private static int serverNum;
	private static ArrayList<ArrayList<SeckillItem>> dbDatas = null;
	
	public static void init(int Num, String ipFile, String seckillplanFile, 
			int threadNum, int queueSize, int gathertime, int timestampcount,
			float payRatio, float cancelRatio,float queryRatio)
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
				//有问题更改
//				dbDatas.get((Integer.parseInt(arr[0])/serverNum)%dbConnectors.size())
//				.add(new SeckillItem(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Integer.parseInt(arr[5])));
				dbDatas.get((Integer.parseInt(arr[0]))%dbConnectors.size())
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
		
		
		dbOperators = new ArrayList<WLDBOperator>();
		for(int i=0;i<dbConnectors.size();i++)
		{
			dbOperators.add(new WLDBOperator(threadNum, queueSize, dbConnectors.get(i), dbDatas.get(i),
					(serverNum*dbConnectors.size()), gathertime, timestampcount,
					payRatio, cancelRatio, queryRatio));
		}
	}
	
	public static WLDBOperator getDbOperator(int slskpkey)
	{
//		return dbOperators.get((slskpkey/serverNum)%dbConnectors.size());
		return dbOperators.get(slskpkey%dbConnectors.size());
	}
	public static WLDBOperator getDbOperator()
	{
		Random random = new Random();
		return dbOperators.get(random.nextInt(dbConnectors.size()));
	}
	public static void main(String[] args) throws FileNotFoundException {
		ServerConfig writeserver = new ServerConfig("/writeServer.properties");
		writeserver.loadProperties();
		WLProceseNode.init(writeserver.getInt("serverNum",1), writeserver.getString("ipfile"), 
				writeserver.getString("seckillplanfile"), writeserver.getInt("threadNum",100),
				writeserver.getInt("queueSize",1000), writeserver.getInt("gathertime",1000),
				writeserver.getInt("timestamp",120),writeserver.getFloat("payRatio", 0),
				writeserver.getFloat("cancelRatio", 0),writeserver.getFloat("queryRatio", 0));
		//开启服务端监听
		new Thread(new WLNettyServer(writeserver.getString("serverport"))).start();
			
		//开一个返回统计信息的netty client
		new Thread(new WLReturnNettyClient(writeserver.getString("clienthost"), writeserver.getString("clientport"))).start();
			
	}
}
