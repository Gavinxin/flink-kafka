package com.ts.lcss;

import java.util.ArrayList;
import java.util.List;

public class TrajectoryLCSS {
	private List<GPSTrack> T1 = new ArrayList<GPSTrack>();
	private List<GPSTrack> T2 = new ArrayList<GPSTrack>();
	private GPSTrack[] LCS;
	private double distThre;
	private double matchRatio;
	private static final double DEFAULT_DISTTHRE = 1;// 经纬度差值阈值大约0.001在地图上相差80-90米
	private int commonLen;

	public TrajectoryLCSS(List<GPSTrack> T1, List<GPSTrack> T2) {
		this.T1 = T1;
		this.T2 = T2;
		this.distThre = DEFAULT_DISTTHRE;
	}

	/**
	 * @param T1
	 * @param T2
	 * @param dist_thre
	 */
	public TrajectoryLCSS(List<GPSTrack> T1, List<GPSTrack> T2, double dist_thre) {
		this(T1, T2);
		this.distThre = dist_thre;
	}

	/**
	 * 动态规划计算所有子问题
	 * 
	 * @return
	 */
	public int[][] getTypeMatrix() {
		int[][] type = new int[T1.size() + 1][T2.size() + 1];
		for (int i = T1.size() - 1; i >= 0; i--) {
			for (int j = T2.size() - 1; j >= 0; j--) {
				if (isClose(T1.get(i), T2.get(j))) {
					//System.out.println(T1.get(i).getLatitude() + " " + T1.get(i).getLongitude());
					//System.out.println(T2.get(j).getLatitude() + " " + T2.get(j).getLongitude());
					type[i][j] = type[i + 1][j + 1] + 1;
					commonLen++;
				} else {
					type[i][j] = Math.max(type[i][j + 1], type[i + 1][j]);
				}
			}
		}
		return type;
	}

	/**
	 * 查看两点是否可以判定为同点
	 * 
	 * @param p1
	 * @param p2
	 * @return
	 */
	public boolean isClose(GPSTrack p1, GPSTrack p2) {
		double x_abs = Math.abs(p1.getLatitude() - p2.getLatitude());
		double y_abs = Math.abs(p1.getLongitude() - p2.getLongitude());
		if (x_abs < distThre && y_abs < distThre)
			return true;
		return false;
	}

	/**
	 * @return
	 */
	public GPSTrack[] genLCSS() {
		int[][] typematrix = getTypeMatrix();
		GPSTrack[] res = new GPSTrack[commonLen];
		int i = 0, j = 0;
		int index = 0;
		while (i < T1.size() && j < T2.size()) {
			if (isClose(T1.get(i), T2.get(j))) {
				//System.out.println(index);
				//System.out.println(i);
				//System.out.println(commonLen);
				//System.out.println(T1.get(i).getLatitude() + " " + T1.get(i).getLongitude());
				//System.out.println(T2.get(j).getLatitude() + " " + T2.get(j).getLongitude());
				res[index++] = T1.get(i);
				i++;
				j++;
			} else if (typematrix[i + 1][j] >= typematrix[i][j + 1]) {
				i++;
			} else {
				j++;
			}
		}
		LCS = res;
		matchRatio = this.LCS.length / (double) (Math.min(T1.size(), T2.size()));
		return res;
	}

	/**
	 * 更新Ratio
	 * 
	 * @return
	 */
	public double getMatchRatio() {
		if (matchRatio == 0) {
			genLCSS();
		}
		return this.LCS.length / (double) (Math.min(T1.size(), T2.size()));
	}

	public static void main(String[] args) {
		List<GPSTrack> T1 = new ArrayList<>();
		List<GPSTrack> T2 = new ArrayList<>();
		T1.add(new GPSTrack(114.300, 30.1));
		T1.add(new GPSTrack(114.302, 30.101));
		T1.add(new GPSTrack(114.3023, 30.1002));
		T1.add(new GPSTrack(114.30235, 30.1011));
		T1.add(new GPSTrack(114.304, 30.1003));
		T2.add(new GPSTrack(114.301, 30.1002));
		T2.add(new GPSTrack(114.3023, 30.1015));
		TrajectoryLCSS lcss = new TrajectoryLCSS(T1, T2, 0.001);
		GPSTrack[] res = lcss.genLCSS();
		System.out.println(lcss.getMatchRatio());
		for (GPSTrack gpsTrack : res) {
			if (gpsTrack != null)
				System.out.println(gpsTrack.getLatitude() + " " + gpsTrack.getLongitude());
		}
	}
}
