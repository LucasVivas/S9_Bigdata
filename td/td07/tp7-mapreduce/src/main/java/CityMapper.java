import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CityMapper
        extends Mapper<Text, TaggedValue, Text, TaggedValue> {
    public void map(Text key, TaggedValue value, Context context
    ) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
