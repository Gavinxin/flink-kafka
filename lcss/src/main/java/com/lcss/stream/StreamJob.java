package com.lcss.stream;

import java.awt.Window;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

public class StreamJob {
	public static  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		public static class MyMapper extends RichMapFunction<GPSTrack, GPSTrack> {
	        /**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			private int pid;

	        @Override
	        public void open(Configuration config) {
	            pid = getRuntimeContext().getIndexOfThisSubtask();
	        }

	        @Override
	        public GPSTrack map(GPSTrack value) throws Exception {
	        	GPSTrack temp = new GPSTrack();
	            value.setNote(Integer.toString(pid));
	            temp = value;
	            return temp;
	        }
		}
	    public static void main(String[] args) throws Exception {
	        // set up the streaming execution environment
	        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	        env.setParallelism(2);
	        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	        env.enableCheckpointing(30000);
	        Properties properties = new Properties();
	        properties.setProperty("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
	        //properties.setProperty("zookeeper.connect", "192.168.0.169:2181");
	        properties.setProperty("group.id", "consumer");
	       // properties.setProperty("flink.partition-discovery.interval-millis", "500");  //check partition auto
	        DataStream<GPSTrack> stream = env.addSource(new FlinkKafkaConsumer010<String>("gpstracks", new SimpleStringSchema(), properties).setStartFromEarliest()).map(new MapFunction<String, GPSTrack>() {
	            /**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				public GPSTrack map(String s) throws Exception {
	                return new GPSTrack(s);
	            }
	        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<GPSTrack>() {
	            /**
				 * TimeStamp
				 */
				private static final long serialVersionUID = 1L;

				@Override
	            public long extractAscendingTimestamp(GPSTrack gpsTracks) {
	                return gpsTracks.getTimeStamp();
	            }
	        }).map(new MyMapper());
	        
	       stream.print();
	        stream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(15))).apply(new AllWindowFunction<GPSTrack,String, TimeWindow>() {


				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				public void apply(TimeWindow window, Iterable<GPSTrack> values, Collector<String> out) throws Exception {
					// TODO Auto-generated method stub
					
					Map<Integer, List<GPSTrack>> map=new HashMap<Integer,List<GPSTrack>>();
					/*ArrayList<GPSTrack> list2=new ArrayList<GPSTrack>();
					values.forEach(t->{
						if(t.getId()==1)
							list1.add(t);
						else
							list2.add(t);
						
					});
					System.out.println(list1.size()+ " " +list2.size());

					TrajectoryLCSS lcss=new TrajectoryLCSS(list1, list2);
					System.out.println(lcss.getMatchRatio());
					System.out.println(df.format(new Date()));
					out.collect(window.getStart()+"  "+window.getEnd());*/
					values.forEach( g->{
						int uid=g.getUid();
						if(map.containsKey(g.getUid())){
							 List<GPSTrack> list=map.get(uid);
							 list.add(g);
							 map.put(uid, list);
						}
						else{
							List<GPSTrack> list= new ArrayList<GPSTrack>();
							list.add(g);
							map.put(uid, list);
						}
					});
					double[][] dis=new double[map.size()][map.size()];
					for(int i=0;i<map.size();i++)
						for(int j=0;j<map.size();j++)
						{
							TrajectoryLCSS lcss=new TrajectoryLCSS(map.get(i), map.get(j));
							dis[i][j]=lcss.getMatchRatio();
						}
					
				
				}
			});


	        env.execute();
	    }
		 
}