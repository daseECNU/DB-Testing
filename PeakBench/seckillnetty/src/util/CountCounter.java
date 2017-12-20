package util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CountCounter {
    public static Map<Integer, Integer> counter(Map<Integer, Integer> metricProp, int showSize) {
        Map<Integer, Integer> result = Maps.newLinkedHashMap();
        List<Integer> times = Lists.newArrayList(metricProp.keySet());
        Collections.sort(times);
        int index = 0; 
        int beforeMax = 0;
        int metric;
        int key;
        for (int i = 0; i < showSize; beforeMax = metric) {
            if (index < times.size()) {
                key = times.get(index++);
                metric = metricProp.get(key);
            } else {
                metric = Math.round((float) (beforeMax / 2));
                key = showSize;
            }
            if(i!=key){
            	List<Integer> data = analyseMetric(beforeMax, metric, i, key, key - i);
		        for (int j = i; j < key; ++j) {
		            result.put(j, Math.abs((data.get(j - i))));
		        }
            }
            i = key;
        }
//        System.out.println(result);
        return result;
    }

    private static List<Integer> analyseMetric(int beforeMax, int max, int start, int end, int size) {
        List<Integer> result = Lists.newArrayList();
        int key = Math.round((max - beforeMax) / (end - start));
        int coefficient;
        coefficient = max - key * end;
        for (int i = start; i < end; ++i) {
            result.add(key * i + coefficient);
        }
        return result;
    }
}
