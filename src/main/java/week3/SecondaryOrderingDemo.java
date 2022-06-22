package week3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondaryOrderingDemo {

    public static class TokenizerMapper
            extends Mapper<Object, Text, DemoWritable, NullWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //value = 2012 01 5
            String[] values = value.toString().split(" ");
            DemoWritable demoWritable = new DemoWritable(values[0] + "-" + values[1], Integer.parseInt(values[2]));
            context.write(demoWritable, NullWritable.get());
        }
    }

    public static class IntSumReducer
            extends Reducer<DemoWritable,NullWritable,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(DemoWritable key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //key1= (2012-01, 05) key2 = (2012-01,30)
            String re = "";
            for (NullWritable e: values) {
                re += " " + String.valueOf(key.getTemperature());
            }
            context.write(new Text(key.getYearMonth()), new Text(re));
        }
    }

    public static void main(String[] args) throws Exception {
        args = new String[] {"/Users/damon/Documents/COMP/大数据/mapreduce/src/main/input/secondaryordering_test.txt"
                , "/Users/damon/Documents/COMP/大数据/mapreduce/src/main/output"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(week3.SecondaryOrderingDemo.class);
        job.setMapperClass(week3.SecondaryOrderingDemo.TokenizerMapper.class);
//        job.setCombinerClass(wordcount.WordCount.IntSumReducer.class);
        job.setReducerClass(week3.SecondaryOrderingDemo.IntSumReducer.class);
//        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(DemoWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(DemoPartitioner.class);
        job.setGroupingComparatorClass(DemoGroupingComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

