import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP5 {
    public static class TP5Mapper
            extends Mapper<LongWritable, Point2DWritable, NullWritable, NullWritable> {

        @Override
        protected void map(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
            if (value.getX() * value.getX() + value.getY() * value.getY() < 1.)
                context.getCounter("Pi", "in").increment(1);
            else
                context.getCounter("Pi", "out").increment(1);
        }
    }

    public static class TP5Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        int nbSplits = Integer.parseInt(args[0]);
        int pointsPerSplits = Integer.parseInt(args[1]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InputFormat compute PI");
        job.setNumReduceTasks(0);
        job.setJarByClass(TP5.class);
        RandomPointInputFormat.setRandomSplits(nbSplits);
        RandomPointInputFormat.setNbPointsPerSplit(pointsPerSplits);
        job.setInputFormatClass(RandomPointInputFormat.class);
        job.setMapperClass(TP5Mapper.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.waitForCompletion(true);
        long inc = job.getCounters().findCounter("Pi","in").getValue();
        long outc = job.getCounters().findCounter("Pi","out").getValue();
        System.out.println("Pi = " + (double)inc * 4. / (double)(outc + inc));
    }
}
