import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapReduceToDecades {

    // TODO: 17/05/2020 add Better combiner, add stop words check.
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] words = line.toString().split("\\s+");
            //Assuming work on 1 gram
            if(words.length == 5) {
                context.write(new Text("1gram:" + words[0] + "\t" + findDecade(words[1])), new IntWritable(Integer.valueOf(words[2])));
                context.write(new Text("Decade:" + findDecade(words[1])), new IntWritable(Integer.valueOf(words[2])));
            }
            else {
                context.write(new Text("2gram:" + words[0] + " " + words[1] + "\t" + findDecade(words[2])), new IntWritable(Integer.valueOf(words[3])));
            }
        }

        private String findDecade(String word) {
            String dec = word.substring(0, word.length() - 1);
            return  dec + "0 - " + dec + "9";
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,LongWritable> {
        private MultipleOutputs<Text, LongWritable> mo;

        public void setup(Context context) {
            mo = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            long decadeCount = 0;
            StringBuilder sKey = new StringBuilder(key.toString());
            if(sKey.toString().startsWith("Decade:"))
            {
                for (IntWritable count: values) {
                    decadeCount += count.get();
                }
                String newKey;
                newKey = sKey.substring("Decade:".length());
                mo.write(new Text(newKey), new LongWritable(decadeCount), "Decs");
            }
            else {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                if(sKey.toString().startsWith("1gram:"))
                    mo.write(new Text(sKey.substring("1gram:".length())), new LongWritable(sum), "1gram");
                else
                    mo.write(new Text(sKey.substring("2gram:".length())), new LongWritable(sum), "2gram");
            }
        }

        public void cleanup(Context context) throws InterruptedException,IOException{
            mo.close();
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = new Job(conf, "gramsUnionAndDecsCalc");
        job.setJarByClass(MapReduceToDecades.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path oneGram = new Path(args[0]);
        Path twoGram = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        MultipleInputs.addInputPath(job, oneGram, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, twoGram, TextInputFormat.class, MapperClass.class);

        // Defines additional single text based output 'text' for the job
        MultipleOutputs.addNamedOutput(job, "Decs", TextOutputFormat.class,
                Text.class, LongWritable.class);

        // Defines additional sequence-file based output 'sequence' for the job
        MultipleOutputs.addNamedOutput(job, "1gram", TextOutputFormat.class,
                Text.class, LongWritable.class);

        MultipleOutputs.addNamedOutput(job, "2gram", TextOutputFormat.class,
                Text.class, LongWritable.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);




    }
}