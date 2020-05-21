import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class AmazonEmrRunner {

    public static void main(String[] args) throws Exception {

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().
                withRegion(Regions.US_EAST_1).build();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
                .withMainClass("MapReduceToDecades.MainClass")
                .withArgs("s3n://2inputs/1gram/", "s3n://2inputs/2gram/", "s3n://2outputbucket/output/"); // A list of command line args passed to the jar files main function when executed

        StepConfig stepConfig = new StepConfig()
                .withName("first map reduce -> make 3 tables.")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("imacKey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()      // collection of steps
                .withName("firstJob")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://yourbucket/logs/");

        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        runFlowRequest.withReleaseLabel("emr-4.2.0");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "...");

        job.setInputFormatClass(SequenceFileInputFormat.class);


    }
}
