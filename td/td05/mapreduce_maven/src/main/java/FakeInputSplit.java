import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FakeInputSplit extends InputSplit implements Writable{
    //the length of the split
    private long size;

    public FakeInputSplit() {
        super();
    }

    public FakeInputSplit(int nbPoints) {
        size = nbPoints;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return size;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public SplitLocationInfo[] getLocationInfo() throws IOException {
        return super.getLocationInfo();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(size);
    }

    public void readFields(DataInput dataInput) throws IOException {
        size = dataInput.readLong();
    }
}
