package com.pojos;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GPSTrack {
	 	
		private static Long i=0L;
		private Long id;
		private int uid;
		private Integer pid;
	    private Long timeStamp;
	    public Integer getPid() {
			return pid;
		}

		public void setPid(Integer pid) {
			this.pid = pid;
		}

		public String getNote() {
			return note;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setLongitude(Double longitude) {
			this.longitude = longitude;
		}

		public void setLatitude(Double latitude) {
			this.latitude = latitude;
		}

		private Date intime;
	    private Date outtime;
	    private Double longitude;
	    private Double latitude;
	    private String note;

	    public Date getIntime() {
			return intime;
		}

		public void setIntime(Date intime) {
			this.intime = intime;
		}

		public Date getOuttime() {
			return outtime;
		}

		public void setOuttime(Date outtime) {
			this.outtime = outtime;
		}

		public void setNote(String note) {
	        this.note = note;
	    }

	    public GPSTrack(String s){
	        String[] tmp=s.split(",");
	        id=i++;
	        uid=Integer.parseInt(tmp[0]);
	        timeStamp=Date2TimeStamp(tmp[1],"yyyy-MM-dd HH:mm:ss");
	        intime=new Date();
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

	    public Long getId() {
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
	        return id + " " + Integer.toString(uid)+" "+timeStamp.toString()+" "+longitude.toString()+" "+latitude.toString()+" "+note;
	    }

		public int getUid() {
			return uid;
		}

		public void setUid(int uid) {
			this.uid = uid;
		}

		public Long getTimeStamp() {
			return timeStamp;
		}

		public void setTimeStamp(Long timeStamp) {
			this.timeStamp = timeStamp;
		}
}
