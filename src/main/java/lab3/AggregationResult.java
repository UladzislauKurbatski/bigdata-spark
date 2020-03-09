package lab3;

import java.io.Serializable;

public class AggregationResult implements Serializable {

    private int bytesCount;

    private int count;

    public AggregationResult() {
    }

    public AggregationResult(int bytesCount, int count) {
        this.bytesCount = bytesCount;
        this.count = count;
    }

    public int getTotalBytesCount() {
        return this.bytesCount;
    }

    public int getCount() {
        return this.count;
    }

    public void increment() {
        this.count++;
    }

    public void addBytesCount(int bytes) {
        this.bytesCount += bytes;
    }

    @Override
    public String toString() {
        return bytesCount + ", " + bytesCount / count;
    }
}
