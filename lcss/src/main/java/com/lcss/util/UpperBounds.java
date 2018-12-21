package com.lcss.util;

import java.util.ArrayList;
import java.util.List;

import com.pojos.GPSTrack;
import com.pojos.MBEBox;

public class UpperBounds {
	private double XLnSpace;
	private double XLaSpace;
	private List<GPSTrack> Q = new ArrayList<GPSTrack>();
	private List<GPSTrack> T = new ArrayList<GPSTrack>();
	
	
	public UpperBounds(double xLnSpace, double xLaSpace, List<GPSTrack> q,
			List<GPSTrack> t) {
		super();
		XLnSpace = xLnSpace;
		XLaSpace = xLaSpace;
		Q = q;
		T = t;
	}
	public UpperBounds() {
		super();
		// TODO Auto-generated constructor stub
	}
	public List<MBEBox> createMBE(){
		List<MBEBox> mblist =new ArrayList<MBEBox>();
		for (GPSTrack g : this.Q) {
			MBEBox mb=new MBEBox();
			mb.setLowerLatitude(g.getLatitude()-this.XLaSpace);
			mb.setUpperLatitude(g.getLatitude()+this.XLaSpace);
			mb.setLowerLongitude(g.getLongitude()-this.XLnSpace);
			mb.setUpperLongitude(g.getLongitude()+this.XLnSpace);
			mblist.add(mb);
		}
		return mblist;
	}
	public int getLCSS_MBE(){
		int count=0;
		List<MBEBox> mblist=this.createMBE();
		for (GPSTrack gpsTrack : T) {
			for (MBEBox mbeBox : mblist) {
				if(gpsTrack.getLatitude()>mbeBox.getLowerLatitude()&&gpsTrack.getLatitude()<mbeBox.getUpperLatitude()&&gpsTrack.getLongitude()>mbeBox.getLowerLongitude()&&gpsTrack.getLongitude()<mbeBox.getUpperLongitude())
					{
						count++;
					}
				
			}
		}
		return count;
	}
}
