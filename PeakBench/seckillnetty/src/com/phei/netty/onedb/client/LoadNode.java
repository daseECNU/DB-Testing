package com.phei.netty.onedb.client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import com.google.common.collect.Maps;
import util.SecKillPlan;
import util.CountCounter;
import util.ServerConfig;
import util.ZipfLaw;

public class LoadNode
{
	private static ArrayList<String> hosts = null;
	private static ArrayList<String> ports = null;
	private static double[] queryRatio = null;
	private static ArrayList<ArrayList<Integer>> skItemsList = null;
	private static ArrayList<ArrayList<Integer>> supplierList = null;
	private static ArrayList<ArrayList<Integer>> typeList = null;
	private static ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;
	private static ZipfLaw<Integer> readzipfLaw = null;
	private static ZipfLaw<SecKillPlan> writezipfLaw = null;
	private static Map<Integer, Integer> ReadmetricProp = Maps.newLinkedHashMap();
	private static Map<Integer, Integer> WritemetricProp = Maps.newLinkedHashMap();
	
	//初始化各种变量
	@SuppressWarnings("unchecked")
	public static void init(String queryRatiostring, String hostports, String skItemsFile,
			String typeListFile, String supplierListFile, int zipfs,
			int zipfN, String readworkloadcountFile, int readworkloadtime,
			String seckillplanFile, String writeworkloadcountFile, int writeworkloadtime)
	{
		String[] queryRatios = queryRatiostring.split(",");
		queryRatio = new double[5];
		queryRatio[0] = Double.parseDouble(queryRatios[0].trim());
		queryRatio[1] = Double.parseDouble(queryRatios[1].trim());
		queryRatio[2] = Double.parseDouble(queryRatios[2].trim());
		queryRatio[3] = Double.parseDouble(queryRatios[3].trim());
		queryRatio[4] = Double.parseDouble(queryRatios[4].trim());
		readzipfLaw = new ZipfLaw<Integer>(zipfs, zipfN);
		
		String[] arr = hostports.split(",");
		hosts = new ArrayList<String>();
		ports = new ArrayList<String>();
		for(int i = 0; i < arr.length; i++) 
		{
			hosts.add(arr[i].split(":")[0].trim());
			ports.add(arr[i].split(":")[1].trim());
		}
		double sum = 0;
		for(int i = 0; i < queryRatio.length; i++) {
			sum += queryRatio[i];
		}
		for(int i = 0; i < queryRatio.length; i++) {
			queryRatio[i] = queryRatio[i] / sum;
			if(i != 0) {
				queryRatio[i] += queryRatio[i - 1];
			}
		}
		BufferedReader br = null;
		int[] skItemsArray = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(skItemsFile)));
			String[] skItemsInfo = br.readLine().split(",");
			skItemsArray = new int[skItemsInfo.length];
			for(int i = 0; i < skItemsInfo.length; i++) {
				skItemsArray[i] = Integer.parseInt(skItemsInfo[i]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null)	br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		skItemsList = new ArrayList<ArrayList<Integer>>();
		Random random = new Random();
		for(int i = 0; i < zipfN; i++)
			skItemsList.add(new ArrayList<Integer>());
		for(int i = 0; i < skItemsArray.length; i++)
			skItemsList.get(random.nextInt(zipfN)).add(skItemsArray[i]);

		ObjectInputStream ois1 = null, ois2 = null;
		try {
			System.out.println("$$$$$$********"+supplierListFile);
			ois1 = new ObjectInputStream(new FileInputStream(supplierListFile));
			ois2 = new ObjectInputStream(new FileInputStream(typeListFile));
			supplierList = (ArrayList<ArrayList<Integer>>)ois1.readObject();
			typeList = (ArrayList<ArrayList<Integer>>)ois2.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(ois1 != null)	ois1.close();
				if(ois2 != null)	ois2.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if(readworkloadtime != 0)
		{
			try
			{
				br = new BufferedReader(new InputStreamReader(new FileInputStream(readworkloadcountFile)));
				String[] loadcount = null; 
				String sringline = null;
				while((sringline = br.readLine()) != null)
				{
					loadcount= sringline.split("=");
					ReadmetricProp.put(Integer.parseInt(loadcount[0].trim()), 
							Integer.parseInt(loadcount[1].trim()));
				}
			}catch (FileNotFoundException e){
				e.printStackTrace();
			}catch (IOException e){
				e.printStackTrace();
			}
			ReadmetricProp = CountCounter.counter(ReadmetricProp, readworkloadtime+1);
		}
		writezipfLaw = new ZipfLaw<SecKillPlan>(zipfs, zipfN);
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(seckillplanFile), "utf-8"));
			
			String inputLine = null;
			ArrayList<SecKillPlan> secKillPlans = new ArrayList<SecKillPlan>();
			while((inputLine = br.readLine()) != null) {
				arr = inputLine.split(",");
				secKillPlans.add(new SecKillPlan(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), 
						Integer.parseInt(arr[5]), Integer.parseInt(arr[7])));
			}
			Collections.sort(secKillPlans);
			
			secKillPlanList = new ArrayList<ArrayList<SecKillPlan>>();
			for(int i = 0; i < zipfN; i++)
				secKillPlanList.add(new ArrayList<SecKillPlan>());
			for(int i = 0; i < secKillPlans.size(); i++)
				secKillPlanList.get(random.nextInt(zipfN)).add(secKillPlans.get(i));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null)	br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if(writeworkloadtime != 0)
		{
			try
			{
				br = new BufferedReader(new InputStreamReader(new FileInputStream(writeworkloadcountFile)));
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
	}
	public static void main(String[] args) throws FileNotFoundException {
		ServerConfig client = new ServerConfig("/Client.properties");
		client.loadProperties();
		LoadNode.init(client.getString("queryRatio"), client.getString("hostports"),
				client.getString("skItemsFile"), client.getString("typeListFile"),
				client.getString("supplierListFile"), client.getInt("zipfs",2), client.getInt("zipfN",10),
				client.getString("readworkloadcountFile"), client.getInt("readworkloadtime", 120), 
				client.getString("seckillplanfile"), client.getString("writeworkloadcountFile"), client.getInt("writeworkloadtime", 120));
		new Thread(new ReturnNettyServer(client.getString("serverport"))).start();
		new Thread(new ResultStatistic(client.getInt("queueSize",100), client.getInt("readworkloadtime",120),
							client.getInt("writeworkloadtime",120), client.getInt("readwriteinterval",100) )).start();		
		new Thread(new NettyClient(hosts, ports)).start();
		if(NettyClient.isAllWritable()){
			new Thread(new LoadThread(client.getFloat("seckillLoadRatio",1), 
					queryRatio, client.getInt("itemKeyRange",10000000), 
					skItemsList, supplierList, typeList, client.getInt("orderKeyRange",10000000), 
					client.getInt("customKeyRange",10000000), secKillPlanList, readzipfLaw, 
					writezipfLaw, ReadmetricProp, WritemetricProp,client.getInt("readworkloadtime",120),
					client.getInt("writeworkloadtime",120), hosts.size(), client.getInt("limitSize",100), 
					client.getInt("readwriteinterval",100))).start();

		}

	}	
}
