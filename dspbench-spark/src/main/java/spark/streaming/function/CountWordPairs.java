package spark.streaming.function;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class CountWordPairs extends BaseFunction implements Function3<String, Optional<Integer>, State<Integer>, Tuple> {

    public CountWordPairs(Configuration config) {
        super(config);
    }

    @Override
    public void Calculate() throws InterruptedException {

    }

    @Override
    public Tuple call(String key, Optional<Integer> count, State<Integer> state) {
        int value = 1;
        if (state.exists()) {
            value = state.get() + 1;
        }
        state.update(value);
        Tuple retTuple = new Tuple();
        retTuple.set("word",key);
        retTuple.set("count",value);
        return retTuple;
    }
}