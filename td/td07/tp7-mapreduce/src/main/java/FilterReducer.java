import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, TaggedValue, Text, TaggedValue> {

    @Override
    protected void reduce(Text key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
