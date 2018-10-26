package com.lcss.util;

public class CompareUtil {
	public static double[] getTop1(double[][] a){
		double[] max=new double[a.length];
		for(int i=0;i<a.length;i++)
		{
			double m=a[i][0];
			for(int j=1;j<a[i].length;j++)
			{
				if(a[i][j]>m)
					m=a[i][j];
			}
			max[i]=m;
		}
		return max;
	}
}
