import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class TP6Reducer
        extends Reducer<NullWritable, City, Text, IntWritable> {
    private int k = 0;
    private TreeSet<City> topKCity = new TreeSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        k = configuration.getInt("k", 10);
    }

    @Override
    protected void reduce(NullWritable key, Iterable<City> values, Context context) throws IOException, InterruptedException {
        for (City c:values) {
            topKCity.add(c.clone());
            if (topKCity.size() > k) {
                topKCity.remove(topKCity.first());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (City city : topKCity.descendingSet()) {
            context.write(new Text(city.getName()), new IntWritable(city.getPop()));
        }
    }
}
