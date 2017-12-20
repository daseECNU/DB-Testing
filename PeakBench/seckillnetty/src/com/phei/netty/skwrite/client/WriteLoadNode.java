package com.phei.netty.skwrite.client;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Maps;

import util.CountCounter;
import util.SecKillPlan;
import util.ServerConfig;
import util.ZipfLaw;

public class WriteLoadNode
{
	private static ArrayList<String> hosts = null;
	private static ArrayList<String> ports = null;
	private static ArrayList<ArrayList<SecKillPlan>> secKillPlanList = null;
	private static ZipfLaw<SecKillPlan> zipfLaw = null;
	private static Map<Integer, Integer> metricProp = Maps.newLinkedHashMap();
	
	public static void init(String hostports, String seckillplanFile, 
			int zipfs, int zipfN, String workloadcountFile, int workloadtime)
	{
		zipfLaw = new ZipfLaw<SecKillPlan>(zipfs, zipfN);
		String[] arr = hostports.split(",");
		hosts = new ArrayList<String>();
		ports = new ArrayList<String>();
		for(int i = 0; i < arr.length; i++) 
		{
			hosts.add(arr[i].split(":")[0].trim());
			ports.add(arr[i].split(":")[1].trim());
		}
		
		BufferedReader br = null;
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
			Random random = new Random();
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
		ServerConfig writeclient = new ServerConfig("/writeClient.properties");
		writeclient.loadProperties();
		WriteLoadNode.init(writeclient.getString("hostports"), writeclient.getString("seckillplanfile"),
				writeclient.getInt("zipfs",2), writeclient.getInt("zipfN",10),
				writeclient.getString("workloadcountFile"), writeclient.getInt("workloadtime", 120));
		
		new Thread(new WLReturnNettyServer(writeclient.getString("serverport"))).start();
		new Thread(new WLResultStatistic(writeclient.getInt("queueSize",100), writeclient.getInt("workloadtime",120) )).start();		
		
		new Thread(new WLNettyClient(hosts, ports)).start();
		if(WLNettyClient.isAllWritable()){
			System.out.println("stert load");
			new Thread(new WriteLoadThread(writeclient.getInt("seckillWriteLoad",95), 
					writeclient.getInt("nonseckillWriteLoad",5), writeclient.getInt("orderKeyRange",10000000), 
					writeclient.getInt("customKeyRange",10000000), writeclient.getInt("itemKeyRange",10000000), 
					secKillPlanList, zipfLaw, metricProp, hosts.size())).start();
		}
	}
}

