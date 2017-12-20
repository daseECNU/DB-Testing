package skread;

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
import java.util.Random;

import util.RanDataGene;

public class UpdateLoadGenerator {

	private int seckillUpdatesPerSec;
	private int nonseckillUpdatesPerSec;
	private String skItemsFile = null;
	private String updateLoadFile = null;
	private int itemKeyRange;
	private int testTimeLength;

	private Integer[] skItemsArray = null;

	public UpdateLoadGenerator(int seckillUpdatesPerSec,
			int nonseckillUpdatesPerSec, String skItemsFile,
			String updateLoadFile, int itemKeyRange, int testTimeLength) {
		super();
		this.seckillUpdatesPerSec = seckillUpdatesPerSec;
		this.nonseckillUpdatesPerSec = nonseckillUpdatesPerSec;
		this.skItemsFile = skItemsFile;
		this.updateLoadFile = updateLoadFile;
		this.itemKeyRange = itemKeyRange;
		this.testTimeLength = testTimeLength;
		init();
	}

	private void init() {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(skItemsFile)));
			String[] skItemsInfo = br.readLine().split(",");
			skItemsArray = new Integer[skItemsInfo.length];
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
	}

	public void generate() {
		BufferedWriter bw = null;
		try {
			int allkillUpdatesPerSec = seckillUpdatesPerSec + nonseckillUpdatesPerSec;
			float skLoadRatio = (float)seckillUpdatesPerSec / (seckillUpdatesPerSec + nonseckillUpdatesPerSec);
			Random random = new Random();
			for(int i = 0; i < testTimeLength; i++) {
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(updateLoadFile + "//" + i + ".txt"), "utf-8"));
				ArrayList<Integer> skItemsList = new ArrayList<Integer>();
				Collections.addAll(skItemsList, skItemsArray);
				Collections.shuffle(skItemsList);
				for(int j = 0; j < allkillUpdatesPerSec; j++) {
					int itemkey = 0;
					if(random.nextFloat() <= skLoadRatio && skItemsList.size() > 0) {
						itemkey = skItemsList.remove(skItemsList.size() - 1);
						bw.write(RanDataGene.getInteger(0, 1000) + "\t" + RanDataGene.getInteger(0, 1000) + "\t" + itemkey + "\n");
					} else {
						itemkey = random.nextInt(itemKeyRange);
						bw.write(RanDataGene.getInteger(0, 1000) + "\t" + itemkey + "\n");
					}
				}
				bw.flush();
			}
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
		InputStream inputStream =new BufferedInputStream(new FileInputStream(userDir +"/staticDataRead.properties"));   
		Properties p = new Properties();   
		try {   
			p.load(inputStream);   
		} catch (IOException e1) {   
		   e1.printStackTrace();   
		}
		UpdateLoadGenerator updateLoadGenerator = new UpdateLoadGenerator(Integer.parseInt(p.getProperty("seckillUpdatesPerSec")), 
				Integer.parseInt(p.getProperty("nonseckillUpdatesPerSec")), p.getProperty("skItemsFile"),
				p.getProperty("updateLoadFile"), Integer.parseInt(p.getProperty("itemKeyRange")), 
				Integer.parseInt(p.getProperty("testTimeLength")));
		updateLoadGenerator.generate();
	}
}
