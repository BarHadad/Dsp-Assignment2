import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class JoinNMR {
    static String ONE_TAG = "#1";
    static String TWO_TAG = "#2";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] words = line.toString().split("\\s+");
            //Assuming work on 1 gram
            if (words.length == 2) { // N table
                Text nKey = new Text(words[0] + ONE_TAG);
                context.write(nKey, new Text(line));
            } else { // Reading from 2Gram, take the second word
                Text twoGramKey = new Text(words[2] + TWO_TAG);
                context.write(twoGramKey, new Text(line));
            }
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, LongWritable> {
        static long curDecadeN;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString().endsWith(ONE_TAG)) {
                curDecadeN = Long.valueOf(values.iterator().next().toString().split("\\s+")[1]);
            } else {
                for (Text val : values) {
                    context.write(val, new LongWritable(curDecadeN));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }

    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = new Job(conf, "joinTables");
        job.setJarByClass(JoinNMR.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path oneGram = new Path(args[0]);
        Path twoGram = new Path(args[1]);
//        Path decs = new Path(args[2]);
        Path outputPath = new Path(args[2]);
        MultipleInputs.addInputPath(job, oneGram, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, twoGram, TextInputFormat.class, MapperClass.class);
//        MultipleInputs.addInputPath(job, decs, TextInputFormat.class, MapperClass.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        // Defines additional single text based output 'text' for the job
//        MultipleOutputs.addNamedOutput(job, "Decs", TextOutputFormat.class,
//                Text.class, LongWritable.class);
//
//        // Defines additional sequence-file based output 'sequence' for the job
//        MultipleOutputs.addNamedOutput(job, "1gram", TextOutputFormat.class,
//                Text.class, LongWritable.class);
//
//        MultipleOutputs.addNamedOutput(job, "2gram", TextOutputFormat.class,
//                Text.class, LongWritable.class);

    }
}
