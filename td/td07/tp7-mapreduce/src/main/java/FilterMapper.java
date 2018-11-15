import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable, Text, Text, TaggedValue> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().isEmpty()) {
            String tokens[] = value.toString().split(",");
            if (tokens.length >= 4) {
                if (NumberUtils.isNumber(tokens[4])) {
                    if (!key.equals(new LongWritable(0)) && !tokens[4].equals("")) {
                        String id = tokens[0]+","+tokens[3];
                        String name = tokens[2];
                        context.write(new Text(id), new TaggedValue(name, true));
                    }
                }
            }
        }
    }
}
