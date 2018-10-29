package com.lcss.util;



import com.ts.lcss.MinHeap;

public class CompareUtil {
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
		double a[][]={{1,2,2,2,2,7,2,2},{2,3,4,5,6,9}};
		double[] b=getTop1(a);
		for (double d : b) {
			System.out.println(d);
		}
		
	}
}
