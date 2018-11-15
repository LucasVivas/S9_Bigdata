import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TP7 {

    public static void main(String[] args) throws Exception {

        System.out.println("-----------------------------------------------");
        for (int i = 0; i < args.length; i++) {
            System.out.println("arg"+i+" : "+args[i]);
        }
        System.out.println("-----------------------------------------------");

        String pathTmpFile = "tmpFileDir";
        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();

        FileSystem.get(conf2).delete(new Path(pathTmpFile), true);

        //job1
        Job job1 = Job.getInstance(conf1, "CityFilter");
        job1.setNumReduceTasks(1);
        job1.setJarByClass(TP7.class);

        //job1 mapper
        job1.setMapperClass(FilterMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(TaggedValue.class);

        //job1 reducer
        job1.setReducerClass(FilterReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(TaggedValue.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);


        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(pathTmpFile));

        job1.waitForCompletion(true);


        Path pathCity = new Path(pathTmpFile + "/part-r-00000");
        Path pathRegion = new Path(args[1]);

        //job2
        Job job2 = Job.getInstance(conf2, "TP7");
        job2.setNumReduceTasks(1);
        job2.setJarByClass(TP7.class);

        MultipleInputs.addInputPath(job2, pathCity, SequenceFileInputFormat.class, CityMapper.class);
        MultipleInputs.addInputPath(job2, pathRegion, TextInputFormat.class, RegionMapper.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(TaggedValue.class);

        //job2 reducer
        job2.setReducerClass(TP7Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));


        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }
}
