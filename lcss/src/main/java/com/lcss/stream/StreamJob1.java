package com.lcss.stream;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import com.lcss.stream.StreamJob.MyMapper;
import com.lcss.util.CompareUtil;
import com.lcss.util.TrajectoryLCSS;
import com.lcss.util.UpperBounds;
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
			return key.getUid() % numPartitions;
		}

	}

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
						}).shuffle().map(new MyMapper()).keyBy(new MyKeyselector<GPSTrack>());

		SingleOutputStreamOperator<Tuple3<Integer, List<Tuple3<Integer, Integer, Integer>>, Map<Integer, ArrayList<GPSTrack>>>> LBstream = stream.timeWindow(Time.seconds(10)).apply(new WindowFunction<GPSTrack, Tuple2<List<Tuple3<Integer, Integer, Integer>>,Map<Integer, ArrayList<GPSTrack>>>, Integer, TimeWindow>() {

			/**
			 * LB Phase 找出每个partition的topk MBE
			 */
			private static final long serialVersionUID = 1L;
			
			@Override
			public void apply(
					Integer key,
					TimeWindow window,
					Iterable<GPSTrack> input,
					Collector<Tuple2<List<Tuple3<Integer, Integer, Integer>>,Map<Integer, ArrayList<GPSTrack>>>> out)
					throws Exception {
				List<Tuple3<Integer, Integer, Integer>> list1 = new ArrayList<Tuple3<Integer, Integer, Integer>>();

				Map<Integer, ArrayList<GPSTrack>> map = new HashMap<Integer, ArrayList<GPSTrack>>();
				input.forEach(g -> {
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
				UpperBounds upbound;
				Iterator<Entry<Integer, ArrayList<GPSTrack>>> entries = map
						.entrySet().iterator();
				// Map<Integer,Tuple2<List<Tuple3<Integer,Integer,Double>>,
				// List<Integer>>> mapout= new HashMap<Integer,
				// Tuple2<List<Tuple3<Integer,Integer,Double>>,List<Integer>>>();
				if (entries.hasNext()) {
					Entry<Integer, ArrayList<GPSTrack>> entry = entries
							.next();
					Iterator<Entry<Integer, ArrayList<GPSTrack>>> entries1 = map
							.entrySet().iterator();
					while (entries1.hasNext()) {
						Entry<Integer, ArrayList<GPSTrack>> entry1 = entries1
								.next();

						if (entry.getKey() != entry1.getKey()) {
							upbound = new UpperBounds(100, 100, entry
									.getValue(), entry1.getValue());
							Tuple3<Integer, Integer, Integer> t = new Tuple3<Integer, Integer, Integer>(
									entry.getKey(), entry1.getKey(),
									upbound.getLCSS_MBE());
							list1.add(t);

						}
					}
					List<Tuple3<Integer, Integer, Integer>> list = CompareUtil.getTop_KSort(
							list1, 2);
					out.collect(new Tuple2<List<Tuple3<Integer, Integer, Integer>>, Map<Integer, ArrayList<GPSTrack>>>(
							list, map));
					// mapout.put(entry.getKey(), new
					// Tuple2<List<Tuple3<Integer,Integer,Double>>,
					// List<Integer>>(list1, list));
				}
				// System.out.println("------------");

			}
		}).map(new RichMapFunction<Tuple2<List<Tuple3<Integer,Integer,Integer>>,Map<Integer,ArrayList<GPSTrack>>>, Tuple3<Integer,List<Tuple3<Integer,Integer,Integer>>,Map<Integer,ArrayList<GPSTrack>>>>() {

			/**
			 * 注入taskid
			 */
			private static final long serialVersionUID = 1L;
			private int taskid;
			@Override
			public void open(Configuration parameters) throws Exception {
				// TODO Auto-generated method stub
				taskid=getRuntimeContext().getIndexOfThisSubtask();
			}
			@Override
			public Tuple3<Integer, List<Tuple3<Integer, Integer, Integer>>, Map<Integer, ArrayList<GPSTrack>>> map(
					Tuple2<List<Tuple3<Integer, Integer, Integer>>, Map<Integer, ArrayList<GPSTrack>>> value)
					throws Exception {
				// TODO Auto-generated method stub
				return new Tuple3<Integer, List<Tuple3<Integer,Integer,Integer>>, Map<Integer,ArrayList<GPSTrack>>>(taskid, value.f0, value.f1);
			}
			
		});
		/*TJAresult=*/LBstream.timeWindowAll(Time.seconds(10)).apply(new AllWindowFunction<Tuple3<Integer, List<Tuple3<Integer,Integer,Integer>>, Map<Integer,ArrayList<GPSTrack>>>, Tuple4<Integer,List<Tuple3<Integer,Integer,Integer>>,Map<Integer,ArrayList<GPSTrack>>,List<Integer>>, TimeWindow>() {

			/**
			 * HJ phase
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void apply(
					TimeWindow window,
					Iterable<Tuple3<Integer, List<Tuple3<Integer, Integer, Integer>>, Map<Integer, ArrayList<GPSTrack>>>> values,
					Collector<Tuple4<Integer, List<Tuple3<Integer, Integer, Integer>>, Map<Integer, ArrayList<GPSTrack>>, List<Integer>>> out)
					throws Exception {
				// TODO Auto-generated method stub
				List<Integer> maxlist=new ArrayList<Integer>();
				//获得全局 maxupperbound
				values.forEach(t->{
					for (Tuple3<Integer, Integer, Integer> tt : t.f1) {
						if(!maxlist.contains(tt.f2))
							maxlist.add(tt.f2);
					}
				});
				//统计topKMBE score
				Map<Integer,Integer> id_scoreMap=new HashMap<Integer, Integer>();
				values.forEach(value->{
					boolean isdeal=false;
					for (Integer k : maxlist) {
						for(Tuple3<Integer, Integer, Integer> t3:value.f1){
							if(k==t3.f1){
								if(id_scoreMap.containsKey(k))
								{
									int temp = 0;
									  temp=id_scoreMap.get(k)+t3.f2;
									id_scoreMap.put(k, temp);
								}
								else{
									id_scoreMap.put(k, t3.f2);
								}
								isdeal=true;
							}
						}
						if(!isdeal){
							if(id_scoreMap.containsKey(k))
							{
								int temp = value.f1.get(value.f1.size()-1).f2;
								  temp=id_scoreMap.get(k)+temp;
								id_scoreMap.put(k, temp);
							}
							else{
								id_scoreMap.put(k, value.f1.get(value.f1.size()-1).f2);
							}
						}
					}
				});
			}
		}).setParallelism(1);
		env.execute("LCSS");
	}
}
