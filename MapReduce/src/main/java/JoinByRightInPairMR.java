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

public class JoinByRightInPairMR {
    static String ONE_GRAM_TAG = "1gram";
    static String TWO_GRAM_TAG = "2gram";
    static String ZERO_GRAM_TAG = "!";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] words = line.toString().trim().split("\\s+");
            // key - [decade] [!] [tag0]
            // key - [decade] [elad] [tag1]
            // key - [decade] [ ] [tab2]
            if (words.length == 2) { // decade total word count table
                Text decadeKey = new Text(words[0] + "\t" + ZERO_GRAM_TAG);
                context.write(decadeKey, new Text(line));
            } else if (words.length == 3) { // Reading from 1Gram
                Text oneGramKey = new Text(words[1] + "\t" + words[0] + "\t" + ONE_GRAM_TAG);
                context.write(oneGramKey, new Text(line));
            } else { // Reading from 2Gram, take the second word
                Text twoGramKey = new Text(words[2] + "\t" + words[1] + "\t" + TWO_GRAM_TAG);
                context.write(twoGramKey, new Text(line));
            }
        }

    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        static String currentKey;
        static long rightGramCounter;
        static long curDecadeN;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            String nonTaggedKey = extractDecade(removeTag(key));
            if (!nonTaggedKey.equals(currentKey)) {
                currentKey = nonTaggedKey;
                rightGramCounter = 0;
                curDecadeN = -1;
            }
            if (key.toString().endsWith(ZERO_GRAM_TAG)) {
                curDecadeN = Long.parseLong(values.iterator().next().toString().split("\\s+")[1]);
            } else if (key.toString().endsWith(ONE_GRAM_TAG)) {
                String[] oneGramData = (values.iterator().next().toString().split("\\s+"));
                rightGramCounter = Long.parseLong(oneGramData[2]);
            } else { //2gram
                for (Text pair : values) {
                    // For example: <itzik shamli 20-29 2> -> <itzik shamli 20-29 2 3> (3 - "shamli" word count)
                    String[] valSplit = pair.toString().split("\\s+");
                    context.write(new Text(valSplit[0] + "\t" + valSplit[1] + "\t" + valSplit[2] + "\t" +
                                    valSplit[3] + "\t" + valSplit[4]),
                            new Text(String.valueOf(rightGramCounter) + "\t" + curDecadeN));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (extractDecade(removeTag(key)).hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static String extractDecade(String untaggedKey) {
        return untaggedKey.split("\\s+")[0];
    }

    public static String removeTag(Text key) {
        if (key.toString().endsWith(ONE_GRAM_TAG))
            return key.toString().substring(0, key.toString().indexOf(ONE_GRAM_TAG)).trim();
        else if (key.toString().endsWith(ZERO_GRAM_TAG))
            return key.toString().substring(0, key.toString().indexOf(ZERO_GRAM_TAG)).trim();
        else return key.toString().substring(0, key.toString().indexOf(TWO_GRAM_TAG)).trim();
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = new Job(conf, "joinTables");
        job.setJarByClass(JoinByRightInPairMR.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
//        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path oneGram = new Path(args[0]);
        Path twoGram = new Path(args[1]);
        Path decs = new Path(args[2]);

        MultipleInputs.addInputPath(job, oneGram, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, twoGram, TextInputFormat.class, MapperClass.class);
        MultipleInputs.addInputPath(job, decs, TextInputFormat.class, MapperClass.class);

        Path outputPath = new Path(args[3]);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
