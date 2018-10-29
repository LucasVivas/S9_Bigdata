import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RandomPointInputFormat extends InputFormat<LongWritable, Point2DWritable> {

    //le nombre de split cree par le inputformat, qui vont etre envoyer au recordReader (RandomPointReader)
    private static int randomSplits = 1;
    //le nombre de points par split
    private static int nbPointsPerSplit = 1;

    public RandomPointInputFormat() {
        super();
    }

    // cree tout (randomSplits) les splits (fakeinputsplit)
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        ArrayList<InputSplit> result = new ArrayList<InputSplit>(randomSplits);
        for (int i=0; i < randomSplits; ++i){
            result.add(new FakeInputSplit(nbPointsPerSplit));
        }
        return result;
    }

    @Override
    public RecordReader<LongWritable, Point2DWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RandomPointReader();
    }

    public static void setRandomSplits(int randomSplits) {
        if (randomSplits > 0) RandomPointInputFormat.randomSplits = randomSplits;
    }

    public static void setNbPointsPerSplit(int nbPointsPerSplit) {
        if (randomSplits > 0) RandomPointInputFormat.nbPointsPerSplit = nbPointsPerSplit;
    }
}
