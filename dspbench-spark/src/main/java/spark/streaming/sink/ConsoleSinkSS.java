package spark.streaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import spark.streaming.constants.BaseConstants;
import spark.streaming.util.Tuple;

public class ConsoleSinkSS extends BaseSinkSS {
    @Override
    public void sinkStream(JavaPairDStream<String, Integer> dt) { //, Configuration conf
        dt.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                System.out.println(record);
                incReceived();
            });
        });
    }

    public void sinkStream2(JavaDStream<Tuple> dt) { //, Configuration conf
        dt.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                System.out.println(record);
                incReceived();
            });
        });
    }
}