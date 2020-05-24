import java.nio.file.Paths;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3Util {
    public static void main(String[] args) {
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
//        s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data


        s3.getObject(GetObjectRequest.builder().bucket("datasets.elasticmapreduce").key("ngrams/books/20090715/eng-us-all/2gram/data").build(),
                ResponseTransformer.toFile(Paths.get("/Users/barhadad/Desktop/2gram")));
    }
}
