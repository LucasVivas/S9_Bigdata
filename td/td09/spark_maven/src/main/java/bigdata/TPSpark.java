package bigdata;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;


public class TPSpark {

    static class createTuple implements PairFunction<String, String, Integer> {
        @Override
        public Tuple2<String, Integer> call(String s) {
            String name;
            int pop = -1;
            String [] line = s.split(",");
            name = line[2];
            if(!line[4].isEmpty() && StringUtils.isNumeric(line[4]))
                pop = Integer.parseInt(line[4]);
            return new Tuple2<>(name, pop);
        }
    }

	public static void main(String[] args) {

		if (args.length != 1){
			throw new RuntimeException("Need the input file in args");
		}

		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		String inputPath = args[0];
		JavaRDD<String> lines;
        JavaPairRDD<String, Integer> cities_tuplet;
        JavaDoubleRDD cities_double;
        StatCounter statCounter;
        int nbExecutor;

        lines = context.textFile(inputPath);

        //exo1
        nbExecutor = context.sc().getExecutorIds().length();
        System.out.println("----------------------------------------------------------");
        System.out.println("Nb Partitions (before modify) : "+lines.getNumPartitions());
        System.out.println("Nb executor : "+nbExecutor);
        lines = lines.repartition(nbExecutor);
        System.out.println("Nb Partitions (after modify) : "+lines.getNumPartitions());
        System.out.println("----------------------------------------------------------");

        //exo2
        System.out.println("----------------------------------------------------------");
        cities_tuplet = lines.mapToPair(new createTuple());
        System.out.println("Nb of lines (before modify) : "+cities_tuplet.count());
        cities_tuplet = cities_tuplet.filter(tup -> tup._2 > -1);
        System.out.println("Nb of lines (after modify) : "+cities_tuplet.count());
        System.out.println("----------------------------------------------------------");

        //exo3
        System.out.println("----------------------------------------------------------");
        cities_double = cities_tuplet.mapToDouble(tup -> tup._2);
        System.out.println("stat : "+cities_double.stats());
        System.out.println("----------------------------------------------------------");

        //exo4
        System.out.println("----------------------------------------------------------");
        statCounter = cities_double.stats();
        double min = statCounter.min();
        double max = statCounter.max();
        int logmin = (int) Math.log10(min);
        int logmax = (int) Math.log10(max);// + 1;

        double [] bucketArray = IntStream.rangeClosed(logmin,logmax)
                .asDoubleStream()
                .boxed()
                .mapToDouble(i -> Math.pow(10,i)).toArray();

        long [] histogram = cities_double.histogram(bucketArray);

        for (int i = 0; i < histogram.length; i++) {
            System.out.println(Math.pow(10,i+1)+"\t"+histogram[i]);
        }
        System.out.println("----------------------------------------------------------");
    }
}
