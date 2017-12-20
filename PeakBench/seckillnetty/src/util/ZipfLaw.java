package util;

import java.util.ArrayList;
import java.util.Random;

public class ZipfLaw<T> {

	private int s;
	private int N;

	public ZipfLaw(int s, int N) {
		super();
		this.s = s;
		this.N = N;
	}

	public double[] getZipfLawData() {
		double[] zipfLawData = new double[N];
		double numerator = 0, denominator = 0;
		for(int i = 1; i <= N; i++) {
			denominator += (1 / Math.pow(i, s));
		}
		for (int i = 1; i <= N; i++) {
			numerator += (1 / Math.pow(i, s));
			zipfLawData[i - 1] = numerator / denominator;
		}
		return zipfLawData;
	}
	
	public T getZipfLawValue(Random random, ArrayList<ArrayList<T>> list, double[] zipfLawData) {
		Float randomValue = random.nextFloat();
		T result = null ;
		for(int i = 0; i < zipfLawData.length; i++) {
			if(randomValue <= zipfLawData[i]) {
				if(list.get(i).size() == 0) {
					randomValue = random.nextFloat();
					i = -1;
					continue;
				}
				result = list.get(i).get(random.nextInt(list.get(i).size()));
				break;
			}
		}
		return result;
	}
}
