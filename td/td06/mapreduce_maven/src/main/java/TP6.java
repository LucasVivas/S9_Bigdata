import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP6 {
    public static void main(String[] args) throws Exception {
        String pathTmpFile = "tmpFileDir";
        Configuration conf = new Configuration();
        Configuration conf2 = new Configuration();

        //job1
        Job job1 = Job.getInstance(conf, "TP6Filter");
        job1.setNumReduceTasks(1);
        job1.setJarByClass(TP6.class);

        //job1 mapper
        job1.setMapperClass(FilterMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(City.class);

        //job1 reducer
        job1.setReducerClass(FilterReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(City.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);


        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(pathTmpFile));

        job1.waitForCompletion(true);

        //job2
        Job job2 = Job.getInstance(conf2, "TP6");
        job2.setNumReduceTasks(1);
        job2.setJarByClass(TP6.class);

        //job2 mapper
        job2.setMapperClass(TP6Mapper.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(City.class);

        //job2 reducer
        job2.setReducerClass(TP6Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(pathTmpFile+"/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        //FileSystem.get(conf2).delete(new Path(pathTmpFile), true);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
