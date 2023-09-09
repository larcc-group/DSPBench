package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class FilterNull<Tuple> extends BaseFunction implements Function<Tuple, Boolean> {

    @Override
    public Boolean call(Tuple input) throws Exception {
        return input != null;
    }

    @Override
    public void Calculate() throws InterruptedException {

    }
}
