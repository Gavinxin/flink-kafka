package com.lcss.stream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import com.pojos.GPSTrack;

public class MyMapper extends RichMapFunction<GPSTrack, GPSTrack>{

	private static final long serialVersionUID = 1L;
	private int pid;
	@Override
	public void open(Configuration config) {
		pid = getRuntimeContext().getIndexOfThisSubtask();
	}

	@Override
	public GPSTrack map(GPSTrack value) throws Exception {
		GPSTrack temp = new GPSTrack();
		value.setPid(pid);
		temp = value;
		return temp;
	}
	

}
