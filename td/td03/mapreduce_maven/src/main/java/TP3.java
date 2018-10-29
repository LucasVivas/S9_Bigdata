import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP3 {
    // extends Mapper<Object, Text, Text, IntWritable>{
    //context.write(value, new IntWritable(1));
  public static class TP3Mapper
       extends Mapper<LongWritable, Text, Text, NullWritable>{
	  @Override
	  public void map(LongWritable key, Text value, Context context
			  ) throws IOException, InterruptedException {

	      String tokens[] = value.toString().split(",");

          Counter c1 = context.getCounter("WCP","nb_cities");
          Counter c2 = context.getCounter("WCP","nb_pop");
          Counter c3 = context.getCounter("WCP","total_pop");

		  if(!key.equals(new LongWritable(0))){
              c1.increment(1);
              if (!tokens[4].equals("")) {
                  c2.increment(1);
                  c3.increment(Long.parseLong(tokens[4]));
                  context.write(value, NullWritable.get());
              }
          }
	  }
  }
/* //good one
  public static class TP3Reducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      context.write(key, new IntWritable(1));
    }
  }*/

    public static class TP3Reducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, new IntWritable(1));
        }
    }

    //good one
  /*public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TP3");
    //job.setNumReduceTasks(0);
    job.setNumReduceTasks(1);//to use reducer
    job.setJarByClass(TP3.class);
    job.setMapperClass(TP3Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(TP3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }*/

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TP3");
        job.setNumReduceTasks(1);
        job.setJarByClass(TP3.class);
        job.setMapperClass(TP3Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setReducerClass(TP3Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
