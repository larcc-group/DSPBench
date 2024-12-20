package spark.streaming.model.gis;

import java.io.Serializable;
import spark.streaming.util.collection.FixedSizeQueue;

public class Road implements Serializable {
    private final int roadID;
    private final FixedSizeQueue<Integer> roadSpeed;
    private int averageSpeed;
    private int count;

    public Road(int roadID) {
        this.roadID = roadID;
        this.roadSpeed = new FixedSizeQueue<>(30);
    }
    public int getRoadID() {
        return roadID;
    }

    public int getAverageSpeed() {
        return averageSpeed;
    }

    public void setAverageSpeed(int averageSpeed) {
        this.averageSpeed = averageSpeed;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
    
    public void incrementCount() {
        this.count++;
    }

    public FixedSizeQueue<Integer> getRoadSpeed() {
        return roadSpeed;
    }

    public boolean addRoadSpeed(int speed) {
        return roadSpeed.add(speed);
    }
    
    public int getRoadSpeedSize() {
        return roadSpeed.size();
    }
}