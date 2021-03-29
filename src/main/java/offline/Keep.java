package offline;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import scala.Tuple2;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.apache.flink.table.api.Expressions.$;

public class Keep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        System.out.println(new Timestamp(new Date().getTime()));
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println(LocalDateTime.now().format(dtf));
        DataStreamSource<Data> data = bsEnv.addSource(new SourceFunction<Data>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Data> ctx) throws Exception {
                Integer i = 1;
                while (isRunning) {
                    synchronized (ctx.getCheckpointLock()) {
                        Data msg;
                        if (i == 1) {
                            msg = new Data("hw", new Timestamp(new Date().getTime()), 1);
                            i = 2;
                        } else {
                            msg = new Data("hr", new Timestamp(new Date().getTime()), 1);
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
        });
        bsTableEnv.createTemporaryView("login_tmp", data);
//        bsTableEnv.sqlQuery("SELECT * FROM login_tmp")
//                .execute()
//                .print();
        //loginTime |                           name |         num |

        bsTableEnv.sqlQuery("SELECT name,count(num) FROM login_tmp GROUP BY TUMBLE(loginTime, INTERVAL '1' MINUTE),name")
                .execute()
               .print();
        bsEnv.execute("data");
    }

}


