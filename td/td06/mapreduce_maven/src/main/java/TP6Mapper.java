import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeSet;

public class TP6Mapper
        extends Mapper<NullWritable, City, NullWritable, City> {
    private int k = 0;
    private TreeSet<City> topKCity = new TreeSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        k = configuration.getInt("k", 10);
    }

    @Override
    public void map(NullWritable key, City value, Context context
    ) throws IOException, InterruptedException {
                        topKCity.add(value.clone());
                        if (topKCity.size() > k) {
                            topKCity.remove(topKCity.first());
                        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (City city : topKCity) {
            context.write(NullWritable.get(), city);
        }
    }
}