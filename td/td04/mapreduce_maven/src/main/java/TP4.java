import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Stat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TP4 {
    public static class TP4Mapper
            extends Mapper<Object, Text, IntWritable, StatisticWritable> {
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            if (!key.equals(new LongWritable(0)) && !tokens[4].equals("")) {
                long population = Long.parseLong(tokens[4]);
                int equivalenceClassLog = (int) Math.log10(population);
                context.write(new IntWritable(equivalenceClassLog), new StatisticWritable(1, population, 0,0));
            }
        }
    }

    public static class TP4Combiner
            extends Reducer<IntWritable, StatisticWritable, IntWritable, StatisticWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<StatisticWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            long sum = 0, min = Long.MAX_VALUE, max = -1;
            for (StatisticWritable valWritable: values) {
                long lval = valWritable.getSum();
                count++;
                sum += lval;
                if (min > lval){
                    min = lval;
                }
                if (max < lval){
                    max = lval;
                }
            }
            context.write(key, new StatisticWritable(count, sum, min, max));
        }
    }

/*    public static class TP4Reducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;
            int equivalenceClass = (int) Math.pow(10, key.get());
            for (IntWritable val : values) {
                //sum += val.get();
                sum++; //more botifule opti
            }
            context.write(new IntWritable(equivalenceClass), new IntWritable(sum));
        }
    }*/

    public static class TP4Reducer
            extends Reducer<IntWritable, StatisticWritable, IntWritable, StatisticWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<StatisticWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int equivalenceClass = (int) Math.pow(10, key.get());

            int count = 0;
            long sum = 0;
            long min = Long.MAX_VALUE;
            long max = 0;

            Counter c1 = context.getCounter("francois", "mycounter");

            for (StatisticWritable part: values
                 ) {
                c1.increment(1);
                count += part.getCount();
                sum += part.getSum();
                if (min > part.getMin()){
                    min = part.getMin();
                }
                if (max < part.getMax()){
                    max = part.getMax();
                }
            }
            long average = sum/count;
            context.write(new IntWritable(equivalenceClass), new StatisticWritable(count, average, min, max));
        }
    }


    public static class StatisticWritable implements Writable {

        public int count;
        public long sum;
        public long min;
        public long max;

        public StatisticWritable(){
            super();
            this.count = 0;
            this.sum = 0;
            this.min = 0;
            this.max = 0;
        }

        @Override
        public String toString() {
            return count+" "+sum+" "+min+" "+max;
        }

        public StatisticWritable(int count, long sum, long min, long max) {
            super();
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(count);
            dataOutput.writeLong(sum);
            dataOutput.writeLong(min);
            dataOutput.writeLong(max);
        }

        public void readFields(DataInput dataInput) throws IOException {
            count = dataInput.readInt();
            sum = dataInput.readLong();
            min = dataInput.readLong();
            max = dataInput.readLong();
        }

        public int getCount() {
            return count;
        }

        public long getSum() {
            return sum;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TP4");
        job.setNumReduceTasks(1);
        job.setJarByClass(TP4.class);

        job.setMapperClass(TP4Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(StatisticWritable.class);

        job.setCombinerClass(TP4Combiner.class);

        job.setReducerClass(TP4Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(StatisticWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

