package table;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Keep_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsTableEnv.executeSql("CREATE TABLE login_tmp" +
                "(" +
                " l_name STRING, " +
                "l_num BIGINT," +
                " l_loginTime BIGINT," +
                " l_ts AS TO_TIMESTAMP(FROM_UNIXTIME(l_loginTime/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                " WATERMARK FOR l_ts AS l_ts - INTERVAL '5' SECOND " +
                ") WITH (" +
                "'connector' = 'kafka'," +
                "'topic' = 'user_login'," +
                "'properties.bootstrap.servers' = '172.12.17.161:9092'" +
                "'format.type' = 'json'" +
                ")");
        //bsTableEnv.sqlQuery("SELECT *  FROM login_tmp").execute().print();
        bsEnv.execute("data");
    }

}


