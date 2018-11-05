package com.lcss.stream;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import PoJo.GPSTrack;

import com.lcss.stream.StreamJob.MyMapper;

public class Performance {
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
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(30000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        //properties.setProperty("zookeeper.connect", "192.168.0.169:2181");
        properties.setProperty("group.id", "test");
       // properties.setProperty("flink.partition-discovery.interval-millis", "500");  //check partition auto
        DataStream<GPSTrack> stream1 = env.addSource(new FlinkKafkaConsumer010<String>("PerformanceTest", new SimpleStringSchema(), properties).setStartFromEarliest()).map(new MapFunction<String, GPSTrack>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public GPSTrack map(String s) throws Exception {
                return new GPSTrack(s);
            }
        }).map(new MyMapper());
       
        SingleOutputStreamOperator<String> result = stream1.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).apply(new AllWindowFunction<GPSTrack,String, TimeWindow>() {


			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			int count =0 ;
			Long delay = 0L;
			public void apply(TimeWindow window, Iterable<GPSTrack> values, Collector<String> out) throws Exception {
				count=0;
				delay= 0L;
				values.forEach(t->{
					delay+=new Date().getTime()-t.getIntime().getTime();
					count++;
				});
				
				out.collect(window.getEnd()-window.getStart()+" " + count + " " + delay*1.0/count +"\r\n");
			}
		});

        result.writeAsText("C:\\Users\\Gavin\\Desktop\\test\\per.txt");
        env.execute();
    }
}
