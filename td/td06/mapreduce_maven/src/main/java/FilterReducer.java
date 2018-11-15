import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, City, NullWritable, City> {

    @Override
    protected void reduce(Text key, Iterable<City> values, Context context) throws IOException, InterruptedException {
        City maxPopCity = new City();
        for (City c:values) {
            if (c.getPop() > maxPopCity.getPop()) {
                maxPopCity = c;
            }
        }
        context.write(NullWritable.get(), maxPopCity);
    }
}
