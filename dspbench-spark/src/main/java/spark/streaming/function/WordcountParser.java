package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.mutable.Iterable;
import spark.streaming.util.Configuration;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;


/**
 * @author luandopke
 */
public class WordcountParser extends BaseFunction implements FlatMapFunction<String, String> {

    @Override
    public void Calculate() throws InterruptedException {
        /*Tuple2<Map<String, Long>, BlockingQueue<String>> d = super.calculateThroughput(throughput, queue);
        throughput = d._1;
        queue = d._2;
        if (queue.size() >= 1) {
            super.SaveMetrics(queue.take());
        }*/
    }
    public WordcountParser(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<String> call(String input) {
       // incReceived();

        List<String> words = new ArrayList<>();

        for (String word : input.split("\\W")) {
            if (!StringUtils.isBlank(word))
                words.add(word);
        }

//        incEmitted(words.size());

        return words.iterator();
    }
}