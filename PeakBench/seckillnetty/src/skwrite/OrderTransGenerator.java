package skwrite;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import util.RanDataGene;

public class OrderTransGenerator {

	private String seckillplanFile = null;
	private String orderTransFile = null;
	private float queuePerformanceExpect;
	private int customerSize;
	
	public OrderTransGenerator(String seckillplanFile, String orderTransFile,
			float queuePerformanceExpect, int customerSize) {
		super();
		this.seckillplanFile = seckillplanFile;
		this.orderTransFile = orderTransFile;
		this.queuePerformanceExpect = queuePerformanceExpect;
		this.customerSize = customerSize;
	}
	
	private class SecKillPlan implements Comparable<SecKillPlan>{
		int skpkey;
		int itemkey;
		int plancount;
		int popularity;

		public SecKillPlan(int skpkey, int itemkey, int plancount,
				int popularity) {
			super();
			this.skpkey = skpkey;
			this.itemkey = itemkey;
			this.plancount = plancount;
			this.popularity = popularity;
		}

		public int compareTo(SecKillPlan s) {
			if(this.popularity > s.popularity) {
				return -1;
			} else if(this.popularity < s.popularity) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	private void generate() {
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(seckillplanFile), "utf-8"));
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(orderTransFile), "utf-8"));
			
			String inputLine = null;
			ArrayList<SecKillPlan> secKillPlans = new ArrayList<SecKillPlan>();
			while((inputLine = br.readLine()) != null) {
				String[] arr = inputLine.split(",");
				secKillPlans.add(new SecKillPlan(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), 
						Integer.parseInt(arr[5]), Integer.parseInt(arr[7])));
			}
			Collections.sort(secKillPlans);
			
			int tmp = secKillPlans.size() / 10;
			ArrayList<String> orderTrans = new ArrayList<String>();
			for(int i = 0; i < secKillPlans.size(); i++) {
				float ratio = 1 + (10 - i / tmp) * queuePerformanceExpect / 10;
				int count = (int)(secKillPlans.get(i).plancount * ratio);
				for(int j = 0; j < count; j++) {
					orderTrans.add(secKillPlans.get(i).skpkey + "," + secKillPlans.get(i).itemkey + 
							"," + RanDataGene.getInteger(0, customerSize - 1) + "\n");
				}
			}
			Collections.shuffle(orderTrans);
			
			for(int i = 0; i < orderTrans.size(); i++) {
				bw.write(orderTrans.get(i));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(br != null)	br.close();
				if(bw != null)	bw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException {
		String userDir = System.getProperty("user.dir");  
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/writeDB.properties"));  
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}
		OrderTransGenerator orderTransGenerator = new OrderTransGenerator(p.getProperty("seckillplanfile"), 
				p.getProperty("ordertransfile"), Float.parseFloat(p.getProperty("queuePerformanceExpect")),
				Integer.parseInt(p.getProperty("customer"))*Integer.parseInt(p.getProperty("scalFactor")));
		orderTransGenerator.generate();
	}

}
