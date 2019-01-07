import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class RandomPointReader extends RecordReader {
    private LongWritable curKey = new LongWritable(0);
    private Point2DWritable curPoint = new Point2DWritable();
    private long nbPoints = 0;

    public RandomPointReader() {
        super();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.nbPoints = inputSplit.getLength();
        curKey.set(-1);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        curKey.set(curKey.get()+1);
        curPoint.setX(Math.random());
        curPoint.setY(Math.random());
        return curKey.get() < this.nbPoints;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        if (curKey.get() > nbPoints) throw new IOException("no more points");
        return curKey;
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        if (curKey.get() > nbPoints) throw new IOException("no more points");
        return curPoint;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (float)((double)curKey.get()/(double)this.nbPoints);
    }

    @Override
    public void close() throws IOException {

    }
}
