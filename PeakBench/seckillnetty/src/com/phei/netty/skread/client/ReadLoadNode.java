package com.phei.netty.skread.client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import com.google.common.collect.Maps;
import util.CountCounter;
import util.ServerConfig;
import util.ZipfLaw;

public class ReadLoadNode
{
	private static ArrayList<String> hosts = null;
	private static ArrayList<String> ports = null;
	private static double[] queryRatio = null;
	private static ArrayList<ArrayList<Integer>> skItemsList = null;
	private static ArrayList<ArrayList<Integer>> supplierList = null;
	private static ArrayList<ArrayList<Integer>> typeList = null;
	private static ZipfLaw<Integer> zipfLaw = null;
	private static Map<Integer, Integer> metricProp = Maps.newLinkedHashMap();
	
	//初始化各种变量
	@SuppressWarnings("unchecked")
	public static void init(String queryRatiostring, String hostports, String skItemsFile,
			String typeListFile, String supplierListFile, int zipfs,
			int zipfN, String workloadcountFile, int workloadtime)
	{
		String[] queryRatios = queryRatiostring.split(",");
		queryRatio = new double[5];
		queryRatio[0] = Double.parseDouble(queryRatios[0].trim());
		queryRatio[1] = Double.parseDouble(queryRatios[1].trim());
		queryRatio[2] = Double.parseDouble(queryRatios[2].trim());
		queryRatio[3] = Double.parseDouble(queryRatios[3].trim());
		queryRatio[4] = Double.parseDouble(queryRatios[4].trim());
		zipfLaw = new ZipfLaw<Integer>(zipfs, zipfN);
		
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
		
		try
		{
			br = new BufferedReader(new InputStreamReader(new FileInputStream(workloadcountFile)));
			String[] loadcount = null; 
			String sringline = null;
			while((sringline = br.readLine()) != null)
			{
				loadcount= sringline.split("=");
				metricProp.put(Integer.parseInt(loadcount[0].trim()), 
						Integer.parseInt(loadcount[1].trim()));
			}
		}catch (FileNotFoundException e){
			e.printStackTrace();
		}catch (IOException e){
			e.printStackTrace();
		}
        metricProp = CountCounter.counter(metricProp, workloadtime+1);
	}
	
	public static void main(String[] args) throws FileNotFoundException {
//		String userDir = System.getProperty("user.dir");  
//		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/readClient.properties"));  
//		Properties p = new Properties();   
//		try {   
//			p.load(inputStream);   
//		} catch (IOException e1) {   
//		   e1.printStackTrace();   
//		}
		ServerConfig readclient = new ServerConfig("/readClient.properties");
		readclient.loadProperties();
		ReadLoadNode.init(readclient.getString("queryRatio"), readclient.getString("hostports"),
				readclient.getString("skItemsFile"), readclient.getString("typeListFile"),
				readclient.getString("supplierListFile") ,readclient.getInt("zipfs",2),readclient.getInt("zipfN",10),
				readclient.getString("workloadcountFile"), readclient.getInt("workloadtime", 120));
		new Thread(new RLReturnNettyServer(readclient.getString("serverport"))).start();
		new Thread(new RLResultStatistic(readclient.getInt("queueSize",100), readclient.getInt("workloadtime",120) )).start();		
		new Thread(new RLNettyClient(hosts, ports)).start();
//		ReadLoadNode.init(p.getProperty("queryRatio"), p.getProperty("hostports"), 
//		p.getProperty("skItemsFile"), p.getProperty("typeListFile"),
//		p.getProperty("supplierListFile"), Integer.parseInt(p.getProperty("zipfs")), 
//		Integer.parseInt(p.getProperty("zipfN")), p.getProperty("workloadcountFile"), 
//		Integer.parseInt(p.getProperty("workloadtime"))); 	
//		new Thread(new RLReturnNettyServer(p.getProperty("serverport"))).start();
//		new Thread(new ResultStatistic(Integer.parseInt(p.getProperty("queueSize")), Integer.parseInt(p.getProperty("workloadtime")) )).start();	
//		new Thread(new RLNettyClient(hosts, ports)).start();
		if(RLNettyClient.isAllWritable()){
			new Thread(new ReadLoadThread(readclient.getInt("seckillReadLoad",95), 
					readclient.getInt("nonseckillReadLoad",5),
					queryRatio, readclient.getInt("itemKeyRange",10000000), 
					skItemsList, supplierList, typeList, zipfLaw, 
					metricProp, hosts.size(), readclient.getInt("limitSize",100))).start();
//			new Thread(new ReadLoadThread(Integer.parseInt(p.getProperty("seckillReadLoad")), 
//					Integer.parseInt(p.getProperty("nonseckillReadLoad")),
//					queryRatio, Integer.parseInt(p.getProperty("itemKeyRange")), 
//					skItemsList, supplierList, typeList, zipfLawData, 
//					metricProp, hosts.size(), Integer.parseInt(p.getProperty("limitSize")))).start();
		}

	}	
}
