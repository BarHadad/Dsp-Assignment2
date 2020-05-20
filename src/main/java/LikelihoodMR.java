import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class LikelihoodMR {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] words = line.toString().split("\\s+");
            //Assuming work on 1 gram
            float ll = (float) Long.valueOf(words[3]) / Long.valueOf(words[6]);
            Text nKey = new Text(words[2] + "\t" + ll);
            context.write(nKey, new Text(line));
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, FloatWritable> {
        static int counter;
        static String curDecade;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (!getDecade(key).equals(curDecade)) {
                curDecade = getDecade(key);
                counter = 0;
            }
            if (counter < 100) {
                for (Text val : values) {
                    String[] value = val.toString().split("\\s+");
                    context.write(new Text(value[2] + "\t" + value[0] + " " + value[1]), new FloatWritable(getLogLikelihood(key)));
                    counter++;
                }
            }
        }

        private String getDecade(Text key) {
            return key.toString().split("\\s+")[0];
        }

        private float getLogLikelihood(Text key) {
            return Float.valueOf(key.toString().split("\\s+")[1]);
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
        Job job = new Job(conf, "top 100 ll per decade");
        job.setJarByClass(LikelihoodMR.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
