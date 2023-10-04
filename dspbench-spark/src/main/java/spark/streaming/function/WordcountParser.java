package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.List;


/**
 * @author luandopke
 */
public class WordcountParser extends BaseFunction implements Function<String, String> {

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
    public String call(String input) {

        if (StringUtils.isBlank(input))
            return null;

        return input;
    }
}