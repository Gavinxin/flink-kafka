package com.lcss.util;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import com.ts.lcss.MinHeap;

public class CompareUtil {
	public static <T> Map<Integer,Double> getTop_K(Map<Integer,Double> map){
		Map<Integer,Double> descmap = sortMapByValueDescending(map);
		return descmap;
	}
	public static  <K, V extends Comparable<? super V>> Map<K, V> sortMapByValueDescending(Map<K, V> map)
    {
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>()
        {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
            {
                int compare = (o1.getValue()).compareTo(o2.getValue());
                return -compare;
            }
        });

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
	public static double[] getTop1(double[][] a){
		double[] min=new double[a.length];
		for(int i=0;i<a.length;i++)
		{
			double m=a[i][0];
			for(int j=1;j<a[i].length;j++)
			{
				if(a[i][j]<m)
					m=a[i][j];
			}
			min[i]=m;
		}
		return min;
	}
	public static double[][] getTop_K(double[][] dis,int k){
		double top_k[][]=new double[dis.length][k];
		for(int i=0;i<dis.length;i++)
		{
			double[] topk = new double[k];
			for(int j = 0;j< k;j++)
			{
				topk[i] = dis[i][j];
			}
			
			// 转换成最小堆
			MinHeap heap = new MinHeap(topk);
			
			// 从k开始，遍历data
			for(int m= k;m<dis.length;i++)
			{
				double root = heap.getRoot();
				
				// 当数据大于堆中最小的数（根节点）时，替换堆中的根节点，再转换成堆
				if(dis[i][m] > root)
				{
					heap.setRoot(dis[i][m]);
				}
			}
			
			for(int n=0;n<topk.length;k++){
				top_k[i][n]=topk[n];
			}
		}
		return top_k;
	}
	public static double[][] getLessThanThreshold(double[][] dis,double t){
		double lt[][]=new double[dis.length][];
		for(int i=0;i<dis.length;i++)
			for(int j=0;j<dis[i].length;j++){
				int k=0;
				if(dis[i][j]<t)
					lt[i][k]=dis[i][j];
			}
		return lt;			
	}
	public static void main(String[] args) {
		Map<Integer,Double> descmap = new HashMap<Integer, Double>();	
		Map<Integer,Double> map = new HashMap<Integer, Double>();	
		descmap.put(1, 2.1);
		descmap.put(2, 2.5);
		descmap.put(4, 8.1);
		map=sortMapByValueDescending(descmap);
		for (Entry<Integer, Double> entry : map.entrySet())
			  System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()); 
	}
	public static List<Tuple3<Integer, Integer, Integer>> getTop_KSort (
			List<Tuple3<Integer, Integer, Integer>> list1, int k) {
		// TODO Auto-generated method stub
		List<Tuple3<Integer, Integer, Integer>> list=new ArrayList<Tuple3<Integer, Integer, Integer>>();
		list1.sort(new Comparator<Tuple3<Integer, Integer, Integer>>() {

			@Override
			public int compare(Tuple3<Integer, Integer, Integer> o1,
					Tuple3<Integer, Integer, Integer> o2) {
				// TODO Auto-generated method stub\
				if(o1.f2>=o2.f2)
					return -1;
				return 1;
			}
		});
		for (int i = 0; i < k; i++) {
			if(i<list1.size())
			  list.add(list1.get(i));
		}
		return list;
	}
	
}
