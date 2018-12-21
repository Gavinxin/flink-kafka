package com.lcss.stream;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;







import com.lcss.util.TrajectoryLCSS;
import com.pojos.GPSTrack;

public class StreamJob {
	public static SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	/*public static class MyPartition implements Partitioner<GPSTrack> {
		*//**
		 * 按照uid分区
		 *//*
		private static final long serialVersionUID = 1L;
		
		@Override
		public int partition(GPSTrack key, int numPartitions) {
			return key.getUid() % numPartitions;
		}
	}*/
	public static class MyKeyselector<T> implements KeySelector<GPSTrack, Integer> {
		/**
		 * key by taskid
		 */
		private static final long serialVersionUID = 1L;
		@Override
		public Integer getKey(GPSTrack value) throws Exception {
			return value.getPid();
		}

		
	}
	public static void main(String[] args) throws Exception {
		//记录一个windows的轨迹
		Map<Integer,ArrayList<GPSTrack>> map=new HashMap<Integer, ArrayList<GPSTrack>>();
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(2);
		// env.enableCheckpointing(30000);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",
				"localhost:9092,localhost:9093,localhost:9094");
		// properties.setProperty("zookeeper.connect", "192.168.0.169:2181");
		properties.setProperty("group.id", "consumer");

		// properties.setProperty("flink.partition-discovery.interval-millis",
		// "500"); //check partition auto
	 KeyedStream<GPSTrack, Integer> stream = env
				.addSource(
						new FlinkKafkaConsumer010<String>("gpstracks",
								new SimpleStringSchema(), properties)
								.setStartFromLatest())
				.map(new RichMapFunction<String, GPSTrack>() {
					private static final long serialVersionUID = 1L;
					@Override
					public GPSTrack map(String s) throws Exception {
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
						}).shuffle()
						.map(new MyMapper())
						.keyBy(new MyKeyselector<GPSTrack>());
	 	Tuple2<Map<Integer, Map<Integer, Integer>>, Map<Integer, Map<Integer, Double>>> t = StreamIteration.Stream(stream, map,2);
	 	/*Iterator<Entry<Integer, Map<Integer, Integer>>> local_topk_MBE_map_entries=t.f0.entrySet().iterator();
	 	Iterator<Entry<Integer, Map<Integer, Double>>> full_topk_lcss_map_entries=t.f1.entrySet().iterator();
		while(local_topk_MBE_map_entries.hasNext()){
			Entry<Integer, Map<Integer, Integer>> entry1 = local_topk_MBE_map_entries.next();
			
		}*/
		env.execute("LCSS");
	}
}
