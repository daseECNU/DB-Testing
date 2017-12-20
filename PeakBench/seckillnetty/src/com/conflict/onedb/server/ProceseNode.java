package com.conflict.onedb.server;

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
	private static int serverNum, cacheType, ipseperationType;
	private static ArrayList<ArrayList<SeckillItem>> dbDatas = null;
	private static ArrayList<DBConnector> readdbConnectors = null;
	private static ArrayList<DBOperator> readdbOperators = null;
	
	public static void init(ServerConfig server, int ipseperationtype, String seckillplanFile, 
			String writeotherworkloadcountFile, int threadNum, int MastercontrolNum, 
			int queueSize, int gathertime, int readtime, int writetime, int readwriteinterval, 
			float payRatio, float cancelRatio, float queryRatio, int writeDataGeneratorType, 
			int conflictWorkloadcount, int readDataGeneratorType, int readWorkloadcount,
			int testPayimpact, int testPaysck, int DBType, int cachetype, int hashMapType, int voltdbnum)
	{
		String ipFile = null;
		String readipFile = null;
		ipseperationType = ipseperationtype;
		cacheType = cachetype;
		if(ipseperationType == 0) {
			ipFile = server.getString("ipfile");
		}
		else{
			ipFile = server.getString("writeipfile");
		}
		BufferedReader ipbr = null;
		try {
			ipbr = new BufferedReader(new InputStreamReader(new FileInputStream(ipFile)));
			String[] ip = null;
			String ipLine = null;
			dbConnectors = new ArrayList<DBConnector>();
			while((ipLine = ipbr.readLine())!=null)
			{
				ip = ipLine.split(",");
				if(DBType==4)
				{
					String allip = null;
					for(int i=0;i<voltdbnum;i++)
					{
						if(i==0)
							allip = ip[i];
						else
							allip += ","+ip[i];
					}
						
					System.out.println("!!!!!!!!*********"+allip+"******"+ip[voltdbnum]);
					dbConnectors.add(new DBConnector(allip, ip[voltdbnum], ip[voltdbnum+1], 
							ip[voltdbnum+2], ip[voltdbnum+3], ip[voltdbnum+4]));
				}
				else{
					dbConnectors.add(new DBConnector(ip[0], ip[1], ip[2], ip[3], ip[4], ip[5]));
				}
				
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
		if(hashMapType == 1)
		{
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
		}
		if(ipseperationType == 1 && (readtime != 0 || readWorkloadcount != 0))
		{
			readipFile = server.getString("readipfile");
			try {
				ipbr = new BufferedReader(new InputStreamReader(new FileInputStream(readipFile)));
				String[] ip = null;
				String ipLine = null;
				readdbConnectors = new ArrayList<DBConnector>();
				while((ipLine = ipbr.readLine())!=null)
				{
					ip = ipLine.split(",");
					if(DBType==4)
					{
						String allip = null;
						for(int i=0;i<voltdbnum;i++)
						{
							if(i==0)
								allip = ip[i];
							else
								allip += ","+ip[i];
						}
							
						System.out.println("!!!!!!!!*********"+allip+"******"+ip[voltdbnum]);
						readdbConnectors.add(new DBConnector(allip, ip[voltdbnum], ip[voltdbnum+1], 
								ip[voltdbnum+2], ip[voltdbnum+3], ip[voltdbnum+4]));
					}
					else{
						readdbConnectors.add(new DBConnector(ip[0], ip[1], ip[2], ip[3], ip[4], ip[5]));
					}
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
			readdbOperators = new ArrayList<DBOperator>();
			for(int i=0;i<readdbConnectors.size();i++)
			{
				readdbOperators.add(new DBOperator(threadNum, MastercontrolNum, queueSize, readdbConnectors.get(i), 
					gathertime, readtime, writetime, readwriteinterval, payRatio, cancelRatio, 
					queryRatio, writeDataGeneratorType, null, (serverNum*readdbConnectors.size()),
					conflictWorkloadcount/readdbConnectors.size(), readDataGeneratorType, readWorkloadcount, 
					testPayimpact, testPaysck, DBType, writeotherworkloadcountFile, cacheType, 0 ));
				new Thread(readdbOperators.get(i)).start();
			}
		}
		dbOperators = new ArrayList<DBOperator>();
		for(int i=0;i<dbConnectors.size();i++)
		{
			if(hashMapType == 1)
				dbOperators.add(new DBOperator(threadNum, MastercontrolNum, queueSize, dbConnectors.get(i), 
						gathertime, readtime, writetime, readwriteinterval, payRatio, cancelRatio, 
						queryRatio, writeDataGeneratorType, dbDatas.get(i), (serverNum*dbConnectors.size()),
						conflictWorkloadcount/dbConnectors.size(), readDataGeneratorType, readWorkloadcount, 
						testPayimpact, testPaysck, DBType, writeotherworkloadcountFile, cacheType, hashMapType));
			else
				dbOperators.add(new DBOperator(threadNum, MastercontrolNum, queueSize, dbConnectors.get(i), 
						gathertime, readtime, writetime, readwriteinterval, payRatio, cancelRatio, 
						queryRatio, writeDataGeneratorType, null, (serverNum*dbConnectors.size()),
						conflictWorkloadcount/dbConnectors.size(), readDataGeneratorType, readWorkloadcount, 
						testPayimpact, testPaysck, DBType, writeotherworkloadcountFile, cacheType, hashMapType));
			new Thread(dbOperators.get(i)).start();
		}
	}
	
	public static DBOperator getDbOperator()
	{
		Random random = new Random();
		return dbOperators.get(random.nextInt(dbConnectors.size()));
	}
	public static DBOperator getreadDbOperator()
	{
		Random random = new Random();
		if(ipseperationType == 0)
			return dbOperators.get(random.nextInt(dbConnectors.size()));
		else{
//			System.out.println("!!!!!!!!!!!!!!readdddddddd*****");
			return readdbOperators.get(random.nextInt(readdbConnectors.size()));
		}
	}
	public static DBOperator getDbOperator(int slskpkey)
	{
		Random random = new Random();
		if(cacheType == 1)
			return dbOperators.get((slskpkey/serverNum)%dbConnectors.size());
		else
			return dbOperators.get(random.nextInt(dbConnectors.size()));
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		ServerConfig server = new ServerConfig("/Server.properties");
		server.loadProperties();
		int threadNum = server.getInt("threadNum",100);
		int minthreadNum = server.getInt("CN", 1)*server.getInt("MastercontrolNum", 5);
		serverNum = server.getInt("serverNum",1);
		ProceseNode.init(server, server.getInt("seperationType",0),
				server.getString("seckillplanfile"), server.getString("writeotherworkloadcountFile"),
				threadNum > minthreadNum ? threadNum : minthreadNum, server.getInt("MastercontrolNum", 5),
				server.getInt("queueSize",100), server.getInt("gathertime",1000),
				server.getInt("readtime",120), server.getInt("writetime",120),
				server.getInt("readwriteinterval",120), server.getFloat("payRatio", 0),
				server.getFloat("cancelRatio", 0), server.getFloat("queryRatio", 0), 
				server.getInt("writeDataGeneratorType",1), server.getInt("conflictWorkloadcount",1000000),
				server.getInt("readDataGeneratorType",1), server.getInt("readWorkloadcount",100000),
				server.getInt("testPayimpact",0), server.getInt("testPaysck",0), server.getInt("DBType",0),
				server.getInt("cacheType",0), server.getInt("hashMapType",0), server.getInt("voltdbnum", 1));
		//		//开启服务端监听
		new Thread(new NettyServer(server.getString("serverport"))).start();
//				
//		//开一个返回统计信息的netty client
		new Thread(new ReturnNettyClient(server.getString("clienthost"), server.getString("clientport"))).start();
		
	}
}
