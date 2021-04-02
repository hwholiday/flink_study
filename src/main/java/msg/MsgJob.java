package msg;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

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
                        SimpleDateFormat ft = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
                        String windowStart = ft.format(context.window().getStart());
                        String windowSEnd = ft.format(context.window().getEnd());
                        String data = "uid :" + integer + " num :" + iterable.iterator().next() + "windowStart :" + windowStart + "windowSEnd :" + windowSEnd;
                        collector.collect(data);
                    }
                }).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                SimpleDateFormat ft = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
                System.out.println("-----------------" + ft.format(new Date()));
                System.out.println(value.toString());
            }
        });
        env.execute("msg");
    }
}