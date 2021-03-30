package offline;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Keep {
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
                        Thread.sleep(1000);
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
        DataStream<Result> d = bsTableEnv.toAppendStream(result, Result.class);
        d.addSink(new SinkFunction<Result>() {
            @Override
            public void invoke(Result value, Context context) throws Exception {
                System.out.println(value.toString());
            }
        });
        bsEnv.execute("data");
    }

}


