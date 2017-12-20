package skwrite;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class writetestdata
{
	private int table1Size;
	private int table2Size;
	private String skWriteFile;

	public writetestdata( int table1Size, int table2Size, String skWriteFile) {
		super();
		this.table1Size = table1Size;
		this.table2Size = table2Size;
		this.skWriteFile = skWriteFile;
	}
	@SuppressWarnings("resource")
	public void generate() {
		BufferedWriter bw = null;
		long i = 0;
		try {
			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//test1.csv")));
		    for(; i < table1Size; i++) {
				bw.write(i + "\n");
			}
			bw.flush();
			
			i = 0;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(skWriteFile + "//test2.csv")));
			for(; i < table2Size; i++) {
				bw.write(i + "," + (int)(Math.random() * table1Size) + "\n");
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
		writetestdata writetestdata = new writetestdata(1000000, 8000000, "//home//postgres//zcx//data");
		writetestdata.generate();
	}
}
