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
	    @Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result
					+ ((intime == null) ? 0 : intime.hashCode());
			result = prime * result
					+ ((latitude == null) ? 0 : latitude.hashCode());
			result = prime * result
					+ ((longitude == null) ? 0 : longitude.hashCode());
			result = prime * result + ((note == null) ? 0 : note.hashCode());
			result = prime * result
					+ ((outtime == null) ? 0 : outtime.hashCode());
			result = prime * result + ((pid == null) ? 0 : pid.hashCode());
			result = prime * result
					+ ((timeStamp == null) ? 0 : timeStamp.hashCode());
			result = prime * result + uid;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			GPSTrack other = (GPSTrack) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (intime == null) {
				if (other.intime != null)
					return false;
			} else if (!intime.equals(other.intime))
				return false;
			if (latitude == null) {
				if (other.latitude != null)
					return false;
			} else if (!latitude.equals(other.latitude))
				return false;
			if (longitude == null) {
				if (other.longitude != null)
					return false;
			} else if (!longitude.equals(other.longitude))
				return false;
			if (note == null) {
				if (other.note != null)
					return false;
			} else if (!note.equals(other.note))
				return false;
			if (outtime == null) {
				if (other.outtime != null)
					return false;
			} else if (!outtime.equals(other.outtime))
				return false;
			if (pid == null) {
				if (other.pid != null)
					return false;
			} else if (!pid.equals(other.pid))
				return false;
			if (timeStamp == null) {
				if (other.timeStamp != null)
					return false;
			} else if (!timeStamp.equals(other.timeStamp))
				return false;
			if (uid != other.uid)
				return false;
			return true;
		}

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
	        return id + " " + Integer.toString(uid)+" "+timeStamp.toString()+" "+longitude.toString()+" "+latitude.toString()+" "+pid;
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
