package com.phei.netty.skread.server;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import util.DBConnector;
import util.ServerConfig;


public class RLProceseNode
{
	private static ArrayList<DBConnector> dbConnectors = null;
	private static ArrayList<RLDBOperator> dbOperators = null;
	
	public static void init(String ipFile,int threadNum, int queueSize, int gathertime, int timestampcount)
	{
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
		dbOperators = new ArrayList<RLDBOperator>();
		for(int i=0;i<dbConnectors.size();i++)
		{
			dbOperators.add(new RLDBOperator(threadNum, queueSize, dbConnectors.get(i), gathertime, timestampcount));
		}
	}
	
	public static RLDBOperator getDbOperator()
	{
		Random random = new Random();
		return dbOperators.get(random.nextInt(dbConnectors.size()));
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		ServerConfig readserver = new ServerConfig("/readServer.properties");
		readserver.loadProperties();
		RLProceseNode.init(readserver.getString("ipfile"), readserver.getInt("threadNum",100),
				readserver.getInt("queueSize",1000), readserver.getInt("gathertime",1000),
				readserver.getInt("timestamp",120));
		//开启服务端监听
		new Thread(new RLNettyServer(readserver.getString("serverport"))).start();
				
		//开一个返回统计信息的netty client
		new Thread(new RLReturnNettyClient(readserver.getString("clienthost"), readserver.getString("clientport"))).start();
		
	}
}
