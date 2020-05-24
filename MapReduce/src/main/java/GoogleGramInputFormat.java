import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class GoogleGramInputFormat extends FileInputFormat<Text,GoogleGram> {
    @Override
    public RecordReader<Text, GoogleGram> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new GoogleGramRecordReader();
    }
}
