package stream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

public class WindowsDataStream_kafka {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.12.17.161:9092");
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);//默认100毫秒
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("user_login", new SimpleStringSchema(), properties);
        DataStream<String> info = env.addSource(myConsumer);
        info.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                String[] data = value.split(":");
                out.collect(Tuple3.of(data[0], Integer.valueOf(data[1]), Integer.valueOf(data[2])));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                        return element.f1 * 1000; //指定EventTime对应的字段
                    }
                }))
                .keyBy(v -> v.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) throws Exception {
                        Tuple3<String, Integer, Integer> data = Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
                        return data;
                    }
                }, new ProcessWindowFunction<Tuple3<String, Integer, Integer>, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<Object> out) throws Exception {
                        Integer count = 0;
                        for (Tuple3<String, Integer, Integer> a : elements) {
                            count += a.f2;
                        }
                        out.collect("用户 : " + s + " login num : " + String.valueOf(count));
                    }
                }).print("xxxxxxxxxxxx :");

        env.execute("Process");
    }

}