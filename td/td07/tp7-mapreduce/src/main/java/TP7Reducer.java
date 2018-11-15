import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TP7Reducer
        extends Reducer<Text,TaggedValue,Text,Text> {
    public void reduce(Text key, Iterable<TaggedValue> values,
                       Context context)
            throws IOException, InterruptedException {
                ArrayList<String> citiesName=new ArrayList<String>();
                String regionName=null;
                for (TaggedValue val: values){
                    if (val.isCity())
                        citiesName.add(val.getName());
                    else
                        regionName = val.getName();
                }
                if (!citiesName.isEmpty() && regionName!=null)
                    for (String val: citiesName)
                        context.write(new Text(regionName), new Text(val));
    }
}