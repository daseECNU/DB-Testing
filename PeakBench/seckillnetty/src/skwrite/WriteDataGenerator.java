package skwrite;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;

import util.RanDataGene;
import util.ZipfLaw;

public class WriteDataGenerator {

	private int scalFactor;
	private int skScalFactor;
	private int skItemCount;
	private long[] tableSize = null;
	private String skWriteFile = null;
	private int zipfs;
	private int zipfN;

	public WriteDataGenerator(int scalFactor, int skScalFactor, int skItemCount,
			long[] tableSize, String skWriteFile, int zipfs, int zipfN) {
		super();
		this.scalFactor = scalFactor;
		this.skScalFactor = skScalFactor;
		this.skItemCount = skItemCount;
		this.tableSize = tableSize;
		this.skWriteFile = skWriteFile;
		this.zipfs = zipfs;
		this.zipfN = zipfN;
	}

	@SuppressWarnings({ "unchecked", "resource", "rawtypes" })
	public void generate() {
		Random random = new Random();
		ZipfLaw zipfLaw = new ZipfLaw(zipfs, zipfN);
		double[] zipfLawData = zipfLaw.getZipfLawData();
		
		BufferedWriter bw = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		ArrayList<ArrayList<Integer>> skitemcountlist = new ArrayList<ArrayList<Integer>>();
		for(int i = 0; i < zipfN; i++)
			skitemcountlist.add(new ArrayList<Integer>());
		for(int i = 10; i <= skItemCount; i++)
			skitemcountlist.get(random.nextInt(zipfN)).add(i);
		
		long i = 0;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//customer.csv")));
			long customerSize = tableSize[0] * scalFactor;
			for(; i < customerSize; i++) {
				bw.write(i + "," + RanDataGene.getString(50) + "," + RanDataGene.getString(100) + "\n");
			}
			bw.flush();

			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//supplier.csv")));
			long supplierSize = tableSize[1] * scalFactor;
			for(; i < supplierSize; i++) {
				bw.write(i + "," + RanDataGene.getString(50) + "," + RanDataGene.getString(100) + "\n");
			}
			bw.flush();

			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//item.csv")));
			long itemSize = tableSize[2] * scalFactor;
			for(; i < itemSize; i++) {
				bw.write(i + "," + RanDataGene.getInteger(0, supplierSize - 1) + "," + RanDataGene.getString(30) + "," + 
						RanDataGene.getInteger(0, 1000) + "," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
						"," + RanDataGene.getString(1000) + "," + RanDataGene.getInteger(0, 1000) + "," + 
						RanDataGene.getInteger(0, 100) + "\n");
			}
			bw.flush();

			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//orders.csv")));
			long ordersSize = tableSize[3] * scalFactor;
			for(; i < ordersSize; i++) {
				
				bw.write(i + "," + RanDataGene.getInteger(0, customerSize - 1) + 
						",0," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
						"," +RanDataGene.getDate(new Date(0), new Date(), sdf) + 
						"," + RanDataGene.getDate(new Date(0), new Date(), sdf) + 
						"," + (random.nextFloat() < 0.8 ? 1 : RanDataGene.getInteger(2, 3)) + "\n");
			}
			bw.flush();

			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//orderitem.csv")));
			long orderitemSize = tableSize[4] * scalFactor;
			HashSet[] orderitem;
			orderitem = new HashSet[(int)ordersSize+1];
			long item_id,order_id ;
			for(; i < ordersSize; i++) {
				orderitem[(int)i] = new HashSet();
				item_id = RanDataGene.getInteger(0, itemSize - 1);
				orderitem[(int)i].add(item_id);
				bw.write(i + "," + item_id + "," + RanDataGene.getInteger(1, 10) + 
						"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + "\n");
			}
			i = 0;

			for(; i < orderitemSize - ordersSize; ) {
				order_id = RanDataGene.getInteger(0, ordersSize - 1);
				item_id = RanDataGene.getInteger(0, itemSize - 1);
				if(orderitem[(int)order_id].add(item_id))
				{
					bw.write( order_id + "," + item_id + "," + RanDataGene.getInteger(1, 10) + "," + 
							(RanDataGene.getInteger(1, 1000) - random.nextFloat()) + "\n");
					i++;
				}
			}
			bw.flush();

			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//seckillplan.csv")));
			long seckillplanSize = tableSize[5] * skScalFactor;
			for(; i < seckillplanSize; i++) {
				bw.write(i + "," + RanDataGene.getInteger(0, itemSize - 1) + 
						"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
						"," + "2016-01-01 00:00:00" + "," + "2016-01-01 00:10:00" + 
						"," + zipfLaw.getZipfLawValue(random, skitemcountlist, zipfLawData) + ",0," + RanDataGene.getInteger(0, 100) + "\n");
			}
//			for(; i < seckillplanSize; i++) {
//				bw.write(i + "," + RanDataGene.getInteger(0, itemSize - 1) + 
//						"," + (RanDataGene.getInteger(1, 1000) - random.nextFloat()) + 
//						"," + "2016-01-01 00:00:00" + "," + "2016-01-01 00:10:00" + 
//						"," + skItemCount + ",0," + RanDataGene.getInteger(0, 100) + "\n");
//			}
			bw.flush();

			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//seckillpay.csv")));
			long seckillpaySize = tableSize[6] * skScalFactor;
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
	}

	public static void main(String[] args) throws FileNotFoundException {
		String userDir = System.getProperty("user.dir");  
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/staticDataWrite.properties"));  
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}
		long[] tableSize = new long[7];
		tableSize[0] = Long.parseLong(p.getProperty("customer"));
		tableSize[1] = Long.parseLong(p.getProperty("supplier"));
		tableSize[2] = Long.parseLong(p.getProperty("item"));
		tableSize[3] = Long.parseLong(p.getProperty("orders"));
		tableSize[4] = Long.parseLong(p.getProperty("orderitem"));
		tableSize[5] = Long.parseLong(p.getProperty("seckillplan"));
		tableSize[6] = Long.parseLong(p.getProperty("seckillpay"));
		WriteDataGenerator dataGenerator = new WriteDataGenerator(Integer.parseInt(p.getProperty("scalFactor")), 
				Integer.parseInt(p.getProperty("skScalFactor")), Integer.parseInt(p.getProperty("skItemCount")), 
				tableSize, p.getProperty("skWriteFile"), Integer.parseInt(p.getProperty("zipfs")),
				Integer.parseInt(p.getProperty("zipfN")));
		dataGenerator.generate();
	}
}
