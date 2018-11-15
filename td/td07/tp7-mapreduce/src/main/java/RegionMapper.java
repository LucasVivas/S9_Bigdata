import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RegionMapper
        extends Mapper<Object, Text, Text, TaggedValue> {
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String [] tokens = value.toString().split(",");
        String id = tokens[0].toLowerCase()+","+tokens[1];
        String name = tokens[2].replace("\"", "");
        context.write(new Text(id), new TaggedValue(name, false));
    }
}