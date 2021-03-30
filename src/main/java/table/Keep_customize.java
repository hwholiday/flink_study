package table;

import com.ibm.icu.text.SimpleDateFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Keep_customize {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        DataStream<Data> info = bsEnv.addSource(new SourceFunction<Data>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Data> ctx) throws Exception {
                Integer i = 1;
                while (isRunning) {
                    synchronized (ctx.getCheckpointLock()) {
                        Data msg;
                        if (i == 1) {
                            msg = new Data("hw", new Date().getTime(), 1);
                            i = 2;
                        } else {
                            msg = new Data("hr", new Date().getTime(), 1);
                            i = 1;
                        }
                        ctx.collect(msg);
                        Thread.sleep(100);
                    }
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Data>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<Data>() {
                    @Override
                    public long extractTimestamp(Data data, long l) {
                        return data.loginTime;
                    }
                }));
        Table table = bsTableEnv.fromDataStream(info, $("loginTime").rowtime(), $("name"), $("num"));
        Table result = table
                .window(Tumble.over(lit(10).seconds()).on($("loginTime")).as("w"))
                .groupBy($("w"), $("name"))
                .select($("name"), $("num").count().as("cnt"), $("w").start().as("start"), $("w").end().as("end"), $("w").rowtime().as("rowTime"));
        DataStream<Tuple2<Boolean, Result>> d = bsTableEnv.toRetractStream(result, Result.class);
        d.addSink(new SinkFunction<Tuple2<Boolean, Result>>() {
            @Override
            public void invoke(Tuple2<Boolean, Result> value, Context context) throws Exception {
                SimpleDateFormat tf = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
                System.out.println("--------------------- " + tf.format(new Date()));
                System.out.println(value.toString());
                System.out.println("++++++++++++++++++++++");
            }
        });
        bsEnv.execute("data");
    }

}


