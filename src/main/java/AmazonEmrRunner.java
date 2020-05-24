import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import java.util.ArrayList;
import java.util.List;

public class AmazonEmrRunner {

    public static void main(String[] args) {

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().
                withRegion(Regions.US_EAST_1).build();
        String [] arguments = new String[]{Properties.firstOneGramPath, Properties.firstTwoGramPath, Properties.firstOutputPath};
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(Properties.firstJarPath) // This should be a full map reduce application.
                .withArgs(arguments); // A list of command line args passed to the jar files main function when executed

        List<StepConfig> stepConfigs = new ArrayList<>();

        StepConfig enableDebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new StepFactory().newEnableDebuggingStep());

        StepConfig stepConfig = new StepConfig()
                .withName("first step")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        stepConfigs.add(enableDebugging);
        stepConfigs.add(stepConfig);


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.7.3").withEc2KeyName(Properties.keyPair)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()      // collection of steps
                .withName("firstJob")                                   //claster name
                .withInstances(instances)
                .withSteps(stepConfigs)
                .withLogUri(Properties.firstLogPath);

        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        runFlowRequest.withReleaseLabel("emr-5.3.0");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);




    }
}
