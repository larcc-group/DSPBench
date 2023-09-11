package spark.streaming.function;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;
import org.apache.spark.api.java.function.Function2;


import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class MovingAverage extends BaseFunction implements  Function2<List<Tuple>, Optional<Tuple>, Optional<Tuple>> {
    private int movingAverageWindow;
    public MovingAverage(Configuration config) {
        super(config);
        movingAverageWindow = config.getInt(SpikeDetectionConstants.Config.MOVING_AVERAGE_WINDOW, 1000);
    }

    @Override
    public void Calculate() throws InterruptedException {
       /* Tuple2<Map<String, Long>, BlockingQueue<String>> d = super.calculateThroughput(throughput, queue);
        throughput = d._1;
        queue = d._2;
        if (queue.size() >= 10) {
            super.SaveMetrics(queue.take());
        }*/
    }

    @Override
    public Optional<Tuple> call(List<Tuple> values, Optional<Tuple> state) throws Exception {
        Tuple newState = state.orElse(new Tuple(values));

        for (Tuple value : values) {
            updateTuple(newState, value, values.get(0).getInt("MOTEID"));
        }

        return Optional.of(newState);
    }

    private void updateTuple(Tuple state, Tuple tuple, Integer key) {
        double value = tuple.getDouble("VALUE");
        double avg = value;

        Moving mov;
        if (state.get("mov") == null) {
            mov = new Moving(key);
            mov.add(value);
        } else {
            mov = (Moving) state.get("mov");

            if (mov.getList().size() > movingAverageWindow - 1) {
                mov.remove();
            }
            mov.add(value);
            avg = mov.getSum() / mov.getList().size();
        }

        state.set("mov",mov);
        state.set("id",key);
        state.set("avg",avg);
        state.set("value",value);
    }
}