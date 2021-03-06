package msg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

import static msg.Msg.*;

public class MsgJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        DataStream<String> infoData = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (isRunning) {
                    synchronized (sourceContext.getCheckpointLock()) {
                        String msg = String.format("{ \n" +
                                "  \"userId\":1,\n" +
                                "  \"msgType\":2,\n" +
                                "  \"to\":100,\n" +
                                "  \"sendTime\":%d,\n" +
                                "}", System.currentTimeMillis());
                        sourceContext.collect(msg);
                        Thread.sleep(1000);
                    }
                }
            }

            @Override
            public void cancel() {

            }

        });
        SingleOutputStreamOperator<Msg> data = infoData.flatMap(new FlatMapFunction<String, Msg>() {
            @Override
            public void flatMap(String s, Collector<Msg> collector) throws Exception {
                Msg msg = new Msg();
                JSONObject object = JSON.parseObject(s);
                msg.userId = object.getIntValue("userId");
                msg.msgType = object.getIntValue("msgType");
                msg.to = object.getIntValue("to");
                msg.sendTime = object.getLongValue("sendTime");
                msg.msgCount = 1L;
                collector.collect(msg);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Msg>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Msg>() {
                    @Override
                    public long extractTimestamp(Msg msg, long l) {
                        return msg.sendTime;
                    }
                }));
        data.keyBy(v -> v.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Msg, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Msg msg, Long aLong) {
                        return aLong + 1;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
                        SimpleDateFormat ft = new SimpleDateFormat("yyyy???MM???dd??? HH???mm???ss???");
                        String windowStart = ft.format(context.window().getStart());
                        String windowSEnd = ft.format(context.window().getEnd());
                        String data = "??????ID :" + integer + " ????????? :" + iterable.iterator().next() + "   windowStart :" + windowStart + "   windowSEnd :" + windowSEnd;
                        collector.collect(data);
                    }
                }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                SimpleDateFormat ft = new SimpleDateFormat("yyyy???MM???dd??? HH???mm???ss???");
                System.out.println("-----------------" + ft.format(new Date()));
                System.out.println(value.toString());
            }
        });
        data.keyBy(v -> v.msgType)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Msg>() {
                    @Override
                    public Msg reduce(Msg msg, Msg t1) throws Exception {
                        msg.msgCount += t1.msgCount;
                        return msg;
                    }
                }, new ProcessWindowFunction<Msg, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Msg> iterable, Collector<String> collector) throws Exception {
                        String data;
                        SimpleDateFormat ft = new SimpleDateFormat("yyyy???MM???dd??? HH???mm???ss???");
                        String windowStart = ft.format(context.window().getStart());
                        String windowSEnd = ft.format(context.window().getEnd());
                        switch (iterable.iterator().next().msgType) {
                            case APPID:
                                data = "APPID :" + integer + " ????????? :" + iterable.iterator().next().msgCount + "   windowStart :" + windowStart + "   windowSEnd :" + windowSEnd;
                            case BROADCAST:
                                data = "BROADCAST :" + integer + " ????????? :" + iterable.iterator().next().msgCount + "   windowStart :" + windowStart + "   windowSEnd :" + windowSEnd;
                            case BROADCAST_GROUP:
                                data = "BROADCAST_GROUP :" + integer + " ????????? :" + iterable.iterator().next().msgCount + "   windowStart :" + windowStart + "   windowSEnd :" + windowSEnd;
                            case ROOM:
                                data = "ROOM :" + integer + " ????????? :" + iterable.iterator().next().msgCount + "   windowStart :" + windowStart + "   windowSEnd :" + windowSEnd;
                            case SINGLE_CHAT:
                                data = "SINGLE_CHAT :" + integer + " ????????? :" + iterable.iterator().next().msgCount + "   windowStart :" + windowStart + "   windowSEnd :" + windowSEnd;
                                break;
                            default:
                                data = "";
                        }
                        collector.collect(data);
                    }
                }).print();
        env.execute("msg");
    }
}