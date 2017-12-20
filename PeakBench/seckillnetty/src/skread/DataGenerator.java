package skread;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import util.RanDataGene;
import util.ZipfLaw;

public class DataGenerator {

	private int seckillItemsNum;
	private int nonseckillItemsNum;
	private int typeNum;
	private int supplierNum;
	private String skReadFile = null;
	private String skItemsFile = null;
	private String typeListFile = null;
	private String supplierListFile = null;
	private int zipfs;
	private int zipfN;

	public DataGenerator(int seckillItemsNum, int nonseckillItemsNum,
			int typeNum, int supplierNum, String skReadFile,
			String skItemsFile, String typeListFile, String supplierListFile,
			int zipfs, int zipfN) {
		super();
		this.seckillItemsNum = seckillItemsNum;
		this.nonseckillItemsNum = nonseckillItemsNum;
		this.typeNum = typeNum;
		this.supplierNum = supplierNum;
		this.skReadFile = skReadFile;
		this.skItemsFile = skItemsFile;
		this.typeListFile = typeListFile;
		this.supplierListFile = supplierListFile;
		this.zipfs = zipfs;
		this.zipfN = zipfN;
	}

	public void generate() {
		Random random = new Random();
		ArrayList<ArrayList<Integer>> typeList = new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Integer>> supplierList = new ArrayList<ArrayList<Integer>>();
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
		
		int itemKeyRange = seckillItemsNum + nonseckillItemsNum;
		float skItemRatio = (float)seckillItemsNum / itemKeyRange;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		BufferedWriter bw1 = null, bw2 = null;
		ObjectOutputStream oos1 = null, oos2 = null;
		try {
			bw1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skReadFile), "utf-8"));
			bw2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skItemsFile), "utf-8"));
			oos1 = new ObjectOutputStream(new FileOutputStream(typeListFile));
			oos2 = new ObjectOutputStream(new FileOutputStream(supplierListFile));
			int count = 1;
			while(count <= itemKeyRange) {
				int suppkey = zipfLaw.getZipfLawValue(random, supplierList, zipfLawData);
				int type = zipfLaw.getZipfLawValue(random, typeList, zipfLawData);
//				int type = count % typeNum;
				if(random.nextFloat() < skItemRatio) {
					bw1.write(count + "," + suppkey + "," + RanDataGene.getString(30) + "," + 
							RanDataGene.getInteger(0, 1000) + "," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
							"," + RanDataGene.getString(1000) + "," + type + "," + RanDataGene.getInteger(0, 100) + 
							"," + RanDataGene.getInteger(0, (int)(supplierNum * 0.2)) + 
							"," + RanDataGene.getInteger(0, 1000) + "," + RanDataGene.getDate(new Date(), new Date(), sdf) + 
							"," + RanDataGene.getDate(new Date(), new Date(), sdf) + "\n");
					bw2.write(count + ",");
				} else {
					bw1.write(count + "," + suppkey + "," + RanDataGene.getString(30) + "," + 
							RanDataGene.getInteger(0, 1000) + "," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
							"," + RanDataGene.getString(1000) + "," + type + "," + RanDataGene.getInteger(0, 100) +",-1,-1,1900-01-01 00:00:01,1900-01-01 00:00:01"+ "\n");
				}
				count++;
			}
			oos1.writeObject(typeList);
			oos2.writeObject(supplierList);
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
		String userDir = System.getProperty("user.dir");  
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/staticDataRead.properties"));   
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}   
		DataGenerator dataGenerator = new DataGenerator(Integer.parseInt(p.getProperty("seckillItemsNum")), 
				Integer.parseInt(p.getProperty("nonseckillItemsNum")), Integer.parseInt(p.getProperty("typeNum")),
				Integer.parseInt(p.getProperty("supplierNum")), p.getProperty("skReadFile"), 
				p.getProperty("skItemsFile"), p.getProperty("typeListFile"), 
				p.getProperty("supplierListFile"), Integer.parseInt(p.getProperty("zipfs")),
				Integer.parseInt(p.getProperty("zipfN")));
		dataGenerator.generate();
		System.out.println("skread db gene time: " + ((System.currentTimeMillis() - time) / 1000) + "s");
	}
}
