package com.lcss.stream;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import com.lcss.util.TrajectoryLCSS;
import com.pojos.GPSTrack;

public class StreamJob1 {
	public static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static class MyPartition implements Partitioner<GPSTrack> {

		/**
		 * 按照uid分区
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int partition(GPSTrack key, int numPartitions) {
			// TODO Auto-generated method stub
			return key.getUid() % numPartitions;
		}

	}

	public static class MyKeyselector<T> implements KeySelector<T, T> {
		@Override
		public T getKey(T value) {
			return value;
		}
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);
		//env.enableCheckpointing(30000);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",
				"localhost:9092,localhost:9093,localhost:9094");
		// properties.setProperty("zookeeper.connect", "192.168.0.169:2181");
		properties.setProperty("group.id", "consumer");

		// properties.setProperty("flink.partition-discovery.interval-millis",
		// "500"); //check partition auto
		DataStream<GPSTrack> stream = env
				.addSource(
						new FlinkKafkaConsumer010<String>("gpstracks",
								new SimpleStringSchema(), properties)
								.setStartFromLatest())
				.map(new RichMapFunction<String, GPSTrack>() {
					/**
				 * 
				 */
					private static final long serialVersionUID = 1L;

					@Override
					public GPSTrack map(String s) throws Exception {
						// TODO Auto-generated method stub
						return new GPSTrack(s);
					}

					
				})
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<GPSTrack>() {
							/**
							 * TimeStamp
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public long extractAscendingTimestamp(
									GPSTrack gpsTracks) {
								return gpsTracks.getTimeStamp();
							}
						});
		stream.partitionCustom(new MyPartition(), new MyKeyselector<GPSTrack>())
				.map(new MyMapper())
				.timeWindowAll(Time.seconds(10), Time.seconds(5)).apply(new AllWindowFunction<GPSTrack, Tuple3<Integer,Integer, Double>, TimeWindow>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
							TimeWindow window,
							Iterable<GPSTrack> values,
							Collector<Tuple3<Integer,Integer, Double>> out)
							throws Exception {
						// TODO Auto-generated method stub
						List<Tuple3<Integer, Integer, Double>> list1=new ArrayList<Tuple3<Integer,Integer,Double>>();
						Map<Integer, ArrayList<Tuple2<Integer, Double>>> mapout=new HashMap<Integer,ArrayList<Tuple2<Integer, Double>>>();
						Map<Integer, ArrayList<GPSTrack>> map = new HashMap<Integer, ArrayList<GPSTrack>>();
						values.forEach(g -> {
							int uid = g.getUid();
							if (map.containsKey(g.getUid())) {
								ArrayList<GPSTrack> list = new ArrayList<GPSTrack>();
								list = map.get(uid);
								list.add(g);
								map.put(uid, list);
							} else {
								ArrayList<GPSTrack> list = new ArrayList<GPSTrack>();
								list.add(g);
								map.put(uid, list);
							}
						});
						
						TrajectoryLCSS lcss;
						/*
						 * for (Integer key : map.keySet()) {
						 * System.out.println("Key = " + key); }
						 */
						Iterator<Entry<Integer, ArrayList<GPSTrack>>> entries = map
								.entrySet().iterator();
						Iterator<Entry<Integer, ArrayList<GPSTrack>>> entries1 = map
								.entrySet().iterator();
						while (entries.hasNext()) {
							Tuple3<Integer,Integer, Double> t=new Tuple3<Integer,Integer, Double>();
							Entry<Integer, ArrayList<GPSTrack>> entry = entries.next();
							while (entries1.hasNext()) {
								Entry<Integer, ArrayList<GPSTrack>> entry1 = entries1.next();
								lcss = new TrajectoryLCSS(entry.getValue(),
										entry1.getValue());
								t.setField(entry.getKey(), 0);
								t.setField(entry1.getKey(), 1);
								t.setField(lcss.getMatchRatio(), 2); 
								out.collect(t);
								/*if (mapout.containsKey(entry.getKey())) {
									ArrayList<Tuple2<Integer, Double>> list = mapout.get(entry.getKey());
									list.add(t);
									mapout.put(entry.getKey(), list);
								} else {
									ArrayList<Tuple2<Integer, Double>> list = new ArrayList<Tuple2<Integer, Double>>();
									list.add(t);
									mapout.put(entry.getKey(), list);
								}*/
							}
						}
						

					}
				}).print().setParallelism(3);
		env.execute("LCSS");
	}

}
