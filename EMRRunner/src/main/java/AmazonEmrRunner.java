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
        List<StepConfig> stepConfigs = new ArrayList<>();

        // basic cluster step
        StepConfig enableDebugging = new StepConfig()
                .withName("Enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(new StepFactory().newEnableDebuggingStep());
        stepConfigs.add(enableDebugging);

        // STEP1
        String[] args1 = {Properties.FirstOneGramPath, Properties.FirstTwoGramPath, Properties.FirstOutputPath};
        stepConfigs.add(buildStep(args1,"FirstCountWithDecadeMR","First count with decade"));

        // STEP2
        String[] args2 = {Properties.JoinByLeftFirstArg, Properties.JoinByLeftSecondArg, Properties.JoinByLeftThirdArg};
        stepConfigs.add(buildStep(args2, "JoinByLeftInPairMR", "join by left in pair"));

        // STEP3
        String[] args3 = {Properties.JoinByRightFirstArg, Properties.JoinByRightSecondArg, Properties.JoinByRightThirdArg};
        stepConfigs.add(buildStep(args3, "JoinByRightInPairMR", "join by right in pair"));

        // STEP4
        String[] args4 = {Properties.JoinNFirstArg, Properties.JoinNSecondArg, Properties.JoinNThirdArg};
        stepConfigs.add(buildStep(args4, "JoinNMR", "join N"));

        // STEP5
        String[] args5 = {Properties.LikelihoodFirstArg, Properties.LikelihoodSecondArg};
        stepConfigs.add(buildStep(args5, "LikelihoodMR", "likelihood top 100"));

        // run
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()      // collection of steps
                .withName("Full Run Big Data")                                   //cluster name
                .withInstances(defineInstances())
                .withSteps(stepConfigs)
                .withLogUri(Properties.firstLogPath);

        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        runFlowRequest.withReleaseLabel("emr-5.3.0");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

    private static JobFlowInstancesConfig defineInstances() {
        return new JobFlowInstancesConfig()
                .withInstanceCount(9)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.7.3").withEc2KeyName(Properties.keyPair)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
    }

    private static StepConfig buildStep(String[] args, String mainClass, String stepName) {
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar(Properties.firstJarPath) // This should be a full map reduce application.
                .withMainClass(mainClass)
                .withArgs(args); // A list of command line args passed to the jar files main function when executed

        return new StepConfig()
                .withName(stepName)
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }
}
