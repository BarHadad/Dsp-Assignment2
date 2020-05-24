import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import static java.lang.Math.*;

public class LikelihoodMR {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] words = line.toString().split("\\s+");
            //Assuming work on 1 gram
            double ll = calculateLogLikelihood(Double.parseDouble(words[4]), Double.parseDouble(words[5]),
                    Double.parseDouble(words[3]), Double.parseDouble(words[6]));
            Text nKey = new Text(words[2] + "\t" + ll);
            context.write(nKey, new Text(line));
        }

        private double calculateLogLikelihood(double c1, double c2, double c12, double N) {
            double p = c2 / N;
            double p1 = c12 / c1;
            double p2 = (c2 - c12) / (N - c1);
            double res =
                    log(L(c12, c1, p)) +
                            log(L(c2 - c12, N - c1, p)) -
                            log(L(c12, c1, p1)) -
                            log(L(c2 - c12, N - c1, p2));
            return res;

        }

        private double L(double k, double n, double x) {
            return (pow(x, k) * pow(1 - x, n - k));
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, DoubleWritable> {
        static int counter;
        static String curDecade;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (!getDecade(key).equals(curDecade)) {
                curDecade = getDecade(key);
                counter = 0;
            }
            for (Text val : values) {
                if (counter == 100) return;
                String[] value = val.toString().split("\\s+");
                context.write(new Text(value[2] + "\t" + value[0] + " " + value[1]),
                        new DoubleWritable(getLogLikelihood(key)));
                counter++;
            }
        }

        private String getDecade(Text key) {
            return key.toString().split("\\s+")[0];
        }

        private double getLogLikelihood(Text key) {
            return Double.parseDouble(key.toString().split("\\s+")[1]);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
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
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
