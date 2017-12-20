package util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RanDataGene {

	public static long getInteger(long min, long max){
		long result = min + (long)((max - min + 1) * Math.random());
		return result;
	}

	public static String getDouble(int exponent, int precision){
		char [] numbers = "0123456789".toCharArray();
		StringBuilder result = new StringBuilder("");
		result.append((Math.random() < 0.5 ? "+" : "-"));
		result.append(numbers[(int)(Math.random() * 10)] + ".");
		precision = (int)(Math.random() * precision);
		for(int i = 0; i < precision; i++){
			result.append(numbers[(int)(Math.random() * 10)]);
		}
		result.append("e" + (Math.random() < 0.5 ? "+" : "-") + ((int)(Math.random() * (exponent + 1))));
		return result.toString();
	}

	//a.b; .b; a.
	public static String getDecimal(int p, int s){
		StringBuilder result = null;
		while(true){
			char [] numbers = "0123456789".toCharArray();
			result = new StringBuilder(Math.random() < 0.5 ? "+" : "-");
			int ranp = (int)(Math.random() * (p - s + 1));
			for(int i = 0; i < ranp; i++){
				result.append(numbers[(int)(Math.random() * 10)]);
			}
			result.append(".");
			int rans = (int)(Math.random() * (s + 1));
			for(int i = 0; i < rans; i++){
				result.append(numbers[(int)(Math.random() * 10)]);
			}
			if(!result.toString().matches("[-+.]+")){
				break;
			}
		}
		return result.toString();
	}

	public static String getDate(Date begin, Date end, SimpleDateFormat sdf){
		long beginLongTime = begin.getTime();
		long endLongTime = end.getTime();
		return sdf.format(new Date((long)(Math.random() * (endLongTime - beginLongTime + 1) + beginLongTime)));
	}

	public static String getString(int length){
		//no tab
		char [] chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".
				toCharArray();
		int randomLength = (int)(Math.random() * (length)+1);
		StringBuilder result = new StringBuilder("");
		for(int i = 0; i<randomLength; i++){
			result.append(chars[(int)(Math.random() * 62)]);
		}
		return result.toString();
	}

	public static char getChar(){
		char [] chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".
				toCharArray();
		return chars[(int)(Math.random() * 62)];
	}

	public static boolean getBoolean(){
		return Math.random() < 0.5 ? true : false;
	}
}
