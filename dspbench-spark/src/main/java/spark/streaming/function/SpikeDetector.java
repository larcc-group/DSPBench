 package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SpikeDetector extends BaseFunction implements Function<Tuple2<Integer, Tuple>, Tuple> {
    private double spikeThreshold;

    @Override
    public void Calculate() throws InterruptedException {
      /*  Tuple2<Map<String, Long>, BlockingQueue<String>> d = super.calculateThroughput(throughput, queue);
        throughput = d._1;
        queue = d._2;
        if (queue.size() >= 10) {
            super.SaveMetrics(queue.take());
        }*/
    }

    public SpikeDetector(Configuration config) {
        super(config);
        spikeThreshold = config.getDouble(SpikeDetectionConstants.Config.SPIKE_DETECTOR_THRESHOLD, 0.03d);
    }

    @Override
    public Tuple call(Tuple2<Integer, Tuple> value) throws Exception {
        Tuple tuple = value._2;
        int deviceID = tuple.getInt("id");
        double movingAverageInstant = tuple.getDouble("avg");
        double nextDouble = tuple.getDouble("value");

        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            Tuple t = new Tuple();
            t.set("deviceID", deviceID);
            t.set("avg", movingAverageInstant);
            t.set("value", nextDouble);
            return t;
        }
        return null;
    }
}