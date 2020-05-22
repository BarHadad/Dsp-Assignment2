//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
//
//import java.io.IOException;
//
//public class GoogleGramRecordReader extends RecordReader<Text, GoogleGram> {
//    protected LineRecordReader reader;
//    protected Text key;
//    protected GoogleGram value;
//
//    public GoogleGramRecordReader() {
//        reader = new LineRecordReader();
//        key = null;
//        value = null;
//    }
//
//    @Override
//    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
//        reader.initialize(inputSplit, taskAttemptContext);
//    }
//
//    @Override
//    public boolean nextKeyValue() throws IOException {
//        if (reader.nextKeyValue()) {
//            String[] split = reader.getCurrentValue().toString().split("\\s+");
//            if (split.length == 5) {
//                key = new Text(split[0]);
//                // 1 gram: word, year, occur, pages, books
//                value = new GoogleGram(split[0], split[1], Long.parseLong(split[2]),
//                        Long.parseLong(split[3]), Long.parseLong(split[4]));
//                return true;
//            } else {
//                // 2-gram: word1, word2, year, occur, pages, books
//                String rawKey = split[0] + "\t" + split[1];
//                key = new Text(rawKey);
//                // 1 gram: word, year, occur, pages, books
//                value = new GoogleGram(rawKey, split[2], Long.parseLong(split[3]),
//                        Long.parseLong(split[4]), Long.parseLong(split[5]));
//                return true;
//            }
//        } else {
//            key = null;
//            value = null;
//            return false;
//        }
//    }
//
//    @Override
//    public Text getCurrentKey() {
//        return key;
//    }
//
//    @Override
//    public GoogleGram getCurrentValue() {
//        return value;
//    }
//
//    @Override
//    public float getProgress() throws IOException {
//        return reader.getProgress();
//    }
//
//    @Override
//    public void close() throws IOException {
//        reader.close();
//    }
//}
