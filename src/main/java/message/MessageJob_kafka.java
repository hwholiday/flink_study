package message;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;

public class MessageJob_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE msg_event\n" +
                "(\n" +
                "    biz_tag     STRING,\n" +
                "    uid         INT,\n" +
                "    event       STRING,\n" +
                "    tag         STRING,\n" +
                "    create_time BIGINT,\n" +
                "    ts AS TO_TIMESTAMP(FROM_UNIXTIME(create_time / 1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "      'connector' = 'kafka',\n" +
                "      'topic' = 'msg_event',\n" +
                "      'properties.bootstrap.servers' = '172.12.17.161:9092',\n" +
                "      'format' = 'json'\n" +
                "      )");
        tableEnv.executeSql("CREATE TABLE msg_event_sk\n" +
                "(\n" +
                "    biz_tag      VARCHAR,\n" +
                "    event        VARCHAR,\n" +
                "    tag          VARCHAR,\n" +
                "    create_time  BIGINT,\n" +
                "    uid          INTEGER,\n" +
                "    num          BIGINT\n" +
                ") WITH (\n" +
                "      'connector' = 'clickhouse',\n" +
                "      'url' = 'clickhouse://172.12.17.161:8123',\n" +
                "      'username' = 'default',\n" +
                "      'password' = '123456',\n" +
                "      'database-name' = 'msg',\n" +
                "      'table-name' = 'msg_event_sk',\n" +
                "      'sink.batch-size' = '2',\n" +
                "      'sink.flush-interval' = '1000',\n" +
                "      'sink.max-retries' = '3',\n" +
                "      'sink.partition-strategy' = 'hash',\n" +
                "      'sink.partition-key' = 'biz_tag',\n" +
                "      'sink.ignore-delete' = 'true'\n" +
                "      )");
//        tableEnv.sqlQuery("SELECT FIRST_VALUE(biz_tag)                  as biz_tag," +
//                "                       FIRST_VALUE(event)                    as event," +
//                "                      FIRST_VALUE(tag)                      as tag," +
//                "                     FIRST_VALUE(create_time)              as create_time," +
//                "                     uid                                   as uid," +
//                "                    COUNT(event)                          as num" +
//                "                FROM msg_event" +
//                "                GROUP BY uid, TUMBLE(ts, INTERVAL '1' MINUTE)").execute().print();
        tableEnv.executeSql("INSERT INTO msg_event_sk\n" +
                "SELECT FIRST_VALUE(biz_tag)                  as biz_tag,\n" +
                "       FIRST_VALUE(event)                    as event,\n" +
                "       FIRST_VALUE(tag)                      as tag,\n" +
                "       FIRST_VALUE(create_time)              as create_time,\n" +
                "       uid                                   as uid,\n" +
                "       COUNT(event)                          as num\n" +
                "FROM msg_event\n" +
                "GROUP BY uid, TUMBLE(ts, INTERVAL '1' MINUTE)");
    }
}
