package com.phei.netty.onedb.server;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;

import util.RanDataGene;
import util.ServerConfig;
import util.ZipfLaw;

public class DataGenerator {

	private int ItemsNum;
	private float seckillRatio;
	private int typeNum;
	private int supplierNum;
	private String skItemsFile = null;
	private String typeListFile = null;
	private String supplierListFile = null;
	private int zipfs;
	private int zipfN;
	private int skItemCount;
	private int[] tableSize = null;
	private String skWriteFile = null;

	public DataGenerator(int ItemsNum, float seckillRatio,
			int typeNum, int supplierNum, String skItemsFile, 
			String typeListFile, String supplierListFile,
			int zipfs, int zipfN, int skItemCount,
			int[] tableSize, String skWriteFile) {
		super();
		this.ItemsNum = ItemsNum;
		this.seckillRatio = seckillRatio;
		this.typeNum = typeNum;
		this.supplierNum = supplierNum;
		this.skItemsFile = skItemsFile;
		this.typeListFile = typeListFile;
		this.supplierListFile = supplierListFile;
		this.zipfs = zipfs;
		this.zipfN = zipfN;
		this.skItemCount = skItemCount;
		this.tableSize = tableSize;
		this.skWriteFile = skWriteFile;
	}

	@SuppressWarnings({ "unchecked", "resource", "rawtypes" })
	public void generate() {
		Random random = new Random();
		ArrayList<ArrayList<Integer>> typeList = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Integer>> supplierList = new ArrayList<ArrayList<Integer>>();
		ArrayList<Integer> skitemList = new ArrayList<Integer>();
		for(int i = 0; i < zipfN; i++)
			typeList.add(new ArrayList<Integer>());
		for(int i = 0; i < typeNum; i++)
			typeList.get(random.nextInt(zipfN)).add(i);
		for(int i = 0; i < zipfN; i++)
			supplierList.add(new ArrayList<Integer>());
		for(int i = 0; i < supplierNum; i++)
			supplierList.get(random.nextInt(zipfN)).add(i);
		
		ZipfLaw<Integer> zipfLaw = new ZipfLaw<Integer>(zipfs, zipfN);
		double[] zipfLawData = zipfLaw.getZipfLawData();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		BufferedWriter bw1 = null, bw2 = null;
		ObjectOutputStream oos1 = null, oos2 = null;
		try {
			bw1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//item.csv"), "utf-8"));
			bw2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skItemsFile), "utf-8"));
			oos1 = new ObjectOutputStream(new FileOutputStream(typeListFile));
			oos2 = new ObjectOutputStream(new FileOutputStream(supplierListFile));
			int count = 1;
			while(count <= ItemsNum) {
				int suppkey = zipfLaw.getZipfLawValue(random, supplierList, zipfLawData);
				int type = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
//				int type = count % typeNum;
				if(random.nextFloat() < seckillRatio) {
					bw1.write(count + "," + suppkey + "," + RanDataGene.getString(30) + "," + 
							RanDataGene.getInteger(0, 1000) + "," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
							"," + RanDataGene.getString(1000) + "," + type + "," + RanDataGene.getInteger(0, 100) +  "\n");
					bw2.write(count + ",");
					skitemList.add(count);
				} else {
					bw1.write(count + "," + suppkey + "," + RanDataGene.getString(30) + "," + 
							RanDataGene.getInteger(0, 1000) + "," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
							"," + RanDataGene.getString(1000) + "," + type + "," + RanDataGene.getInteger(0, 100) + "\n");
				}
				count++;
			}
			oos1.writeObject(typeList);
			oos2.writeObject(supplierList);
			
			BufferedWriter bw = null;
			ArrayList<ArrayList<Integer>> skitemcountlist = new ArrayList<ArrayList<Integer>>();
			for(int i = 0; i < zipfN; i++)
				skitemcountlist.add(new ArrayList<Integer>());
			for(int i = 10; i <= skItemCount; i++)
				skitemcountlist.get(random.nextInt(zipfN)).add(i);
			
			long i = 0;
			try {
//				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//customer.csv"), "utf-8"));
//				long customerSize = tableSize[0];
//				for(; i < customerSize; i++) {
//					bw.write(i + "," + RanDataGene.getString(50) + "," + RanDataGene.getString(100) + "\n");
//				}
//				bw.flush();
//
//				i = 0;
//				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//supplier.csv"), "utf-8"));
//				long supplierSize = tableSize[1];
//				for(; i < supplierSize; i++) {
//					bw.write(i + "," + RanDataGene.getString(50) + "," + RanDataGene.getString(100) + "\n");
//				}
//				bw.flush();
//
//				i = 0;
//				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//orders.csv")));
//				long ordersSize = tableSize[3];
//				for(; i < ordersSize; i++) {
//					
//					bw.write(i + "," + RanDataGene.getInteger(0, customerSize - 1) + 
//							",0," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
//							"," +RanDataGene.getDate(new Date(0), new Date(), sdf) + 
//							"," + RanDataGene.getDate(new Date(0), new Date(), sdf) + 
//							"," + (random.nextFloat() < 0.8 ? 1 : RanDataGene.getInteger(2, 3)) + "\n");
//				}
//				bw.flush();
//
//				i = 0;
//				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//orderitem.csv")));
//				long orderitemSize = tableSize[4];
//				HashSet[] orderitem;
//				orderitem = new HashSet[(int)ordersSize+1];
//				long item_id,order_id ;
//				for(; i < ordersSize; i++) {
//					orderitem[(int)i] = new HashSet();
//					item_id = RanDataGene.getInteger(0, ItemsNum - 1);
//					orderitem[(int)i].add(item_id);
//					bw.write(i + "," + item_id + "," + RanDataGene.getInteger(1, 10) + 
//							"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + "\n");
//				}
//				i = 0;
//
//				for(; i < orderitemSize - ordersSize; ) {
//					order_id = RanDataGene.getInteger(0, ordersSize - 1);
//					item_id = RanDataGene.getInteger(0, ItemsNum - 1);
//					if(orderitem[(int)order_id].add(item_id))
//					{
//						bw.write( order_id + "," + item_id + "," + RanDataGene.getInteger(1, 10) + "," + 
//								(RanDataGene.getInteger(1, 1000) - random.nextFloat()) + "\n");
//						i++;
//					}
//				}
//				bw.flush();

				i = 0;
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//seckillplan.csv")));
				long seckillplanSize = tableSize[5];
//				for(; i < seckillplanSize; i++) {
//					bw.write(i + "," + skitemList.get((int) RanDataGene.getInteger(0, skitemList.size()-1)) + 
//							"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
//							"," + "2016-01-01 00:00:00" + "," + "2016-01-01 00:10:00" + 
//							"," + zipfLaw.getZipfLawValue(random, skitemcountlist, zipfLawData) + ",0," + RanDataGene.getInteger(0, 100) + "\n");
//				}
				for(; i < seckillplanSize; i++) {
					if(i<seckillplanSize/5)
						bw.write(i + "," + skitemList.get((int) RanDataGene.getInteger(0, skitemList.size()-1)) + 
								"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
								"," + "2016-01-01 00:00:00" + "," + "2016-01-01 00:10:00" + 
								",100000,0," + RanDataGene.getInteger(0, 100) + "\n");
					else
						bw.write(i + "," + skitemList.get((int) RanDataGene.getInteger(0, skitemList.size()-1)) + 
								"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
								"," + "2016-01-01 00:00:00" + "," + "2016-01-01 00:10:00" + 
								",0,0," + RanDataGene.getInteger(0, 100) + "\n");
				}
//				for(; i < seckillplanSize; i++) {
//					bw.write(i + "," + RanDataGene.getInteger(0, itemSize - 1) + 
//							"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
//							"," + "2016-01-01 00:00:00" + "," + "2016-01-01 00:10:00" + 
//							"," + skItemCount + ",0," + RanDataGene.getInteger(0, 100) + "\n");
//				}
				bw.flush();

				i = 0;
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//seckillpay.csv")));
				long seckillpaySize = tableSize[5];
				for(; i < seckillpaySize; i++) {
					bw.write(i + ",0,2016-01-01 00:00:00,2016-01-01 00:10:00\n");
				}
				bw.flush();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if(bw != null)	bw.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(bw1 != null)	bw1.close();
				if(bw2 != null)	bw2.close();
				if(oos1 != null)	oos1.close();
				if(oos2 != null)	oos2.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException{
		long time = System.currentTimeMillis();
		ServerConfig Datagener = new ServerConfig("/staticData.properties");
		Datagener.loadProperties();
		int[] tableSize = new int[7];
		tableSize[0] = Datagener.getInt("customer", 80000) * Datagener.getInt("scalFactor", 10);
		tableSize[1] = Datagener.getInt("supplier", 500) * Datagener.getInt("scalFactor", 10);
		tableSize[2] = Datagener.getInt("item", 100000) * Datagener.getInt("scalFactor", 10);
		tableSize[3] = Datagener.getInt("orders", 400000) * Datagener.getInt("scalFactor", 10);
		tableSize[4] = Datagener.getInt("orderitem", 800000) * Datagener.getInt("scalFactor", 10);
		tableSize[5] = Datagener.getInt("seckill", 100) * Datagener.getInt("skScalFactor", 10);
		DataGenerator dataGenerator = new DataGenerator(tableSize[2], 
				Datagener.getFloat("seckillRatio", 1), Datagener.getInt("typeNum", 500),
				tableSize[1], Datagener.getString("skItemsFile"), Datagener.getString("typeListFile"), 
				Datagener.getString("supplierListFile"), Datagener.getInt("zipfs", 2),
				Datagener.getInt("zipfN", 10), Datagener.getInt("skItemCount", 1000), 
				tableSize, Datagener.getString("skWriteFile"));
		dataGenerator.generate();
		System.out.println("skread db gene time: " + ((System.currentTimeMillis() - time) / 1000) + "s");
	}
}
