package com.ts.lcss;

import java.text.SimpleDateFormat;

public class GPSTrack {
	 	

		private int id;
	    private Long timeStamp;
	    private Double longitude;
	    private Double latitude;
	    private String note;

	    public void setNote(String note) {
	        this.note = note;
	    }

	    public GPSTrack(String s){
	        String[] tmp=s.split(",");
	        id=Integer.parseInt(tmp[0]);
	        timeStamp=Date2TimeStamp(tmp[1],"yyyy-MM-dd HH:mm:ss");
	        longitude=Double.parseDouble(tmp[2]);
	        latitude=Double.parseDouble(tmp[3]);
	    }
	    public GPSTrack(){

	    }

	    public GPSTrack(Double longitude, Double latitude) {
			super();
			this.longitude = longitude;
			this.latitude = latitude;
		}

		public Double getLatitude() {
	        return latitude;
	    }

	    public Double getLongitude() {
	        return longitude;
	    }

	    public int getId() {
	        return id;
	    }
	    private static Long Date2TimeStamp(String dateStr, String format) {
	        try {
	            SimpleDateFormat sdf = new SimpleDateFormat(format);
	            return sdf.parse(dateStr).getTime() / 1000;
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        return 1L;
	    }

	    @Override
	    public String toString() {
	        return Integer.toString(id)+" "+timeStamp.toString()+" "+longitude.toString()+" "+latitude.toString()+" "+note;
	    }

		public Long getTimeStamp() {
			return timeStamp;
		}

		public void setTimeStamp(Long timeStamp) {
			this.timeStamp = timeStamp;
		}
}
