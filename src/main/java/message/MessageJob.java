package message;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

public class MessageJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.12.17.161:9092");
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("msg_event", new SimpleStringSchema(), properties);
        DataStream<String> info = env.addSource(myConsumer);
        SingleOutputStreamOperator<Message> JsonStreamData = info.flatMap(new FlatMapFunction<String, Message>() {
            @Override
            public void flatMap(String value, Collector<Message> out) throws Exception {
                Message msg = new Message();
                JSONObject object = JSON.parseObject(value);
                msg.setBizTag(object.getString("biz_tag"));
                msg.setUid(object.getIntValue("uid"));
                msg.setCreateTime(object.getLongValue("create_time"));
                msg.setEvent(object.getString("event"));
                msg.setTag(object.getString("tag"));
                out.collect(msg);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.
                <Message>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Message>() {
                    @Override
                    public long extractTimestamp(Message element, long recordTimestamp) {
                        return element.CreateTime;
                    }
                })
        );
        env.execute();
    }
}
