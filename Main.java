package org.example;

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException {

        AWSCredentials credentials_profile = null;
        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and the profile name is specified within it.",
                    e);
        }
        AWSCredentials credentials = credentials_profile;
        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

        //Step1
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://hypernym-detection/step1.jar") // This should be a full map reduce application.
                .withArgs("s3://hypernym-detection/biarcs.00-of-99", "s3://hypernym-detection/output-files/output1");
        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Step2
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3://hypernym-detection/step2.jar") // This should be a full map reduce application.
                .withArgs("s3://hypernym-detection/output-files/output1", "s3://hypernym-detection/output-files/output2");
        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        Map<String,String> hiveProperties1 = new HashMap<String,String>();
        Map<String,String> hiveProperties2 = new HashMap<String,String>();
        hiveProperties1.put("JAVA_HOME","/usr/lib/jvm/java-11-amazon-corretto.x86_64");
        hiveProperties2.put("spark.executorEnv.JAVA_HOME","/usr/lib/jvm/java-11-amazon-corretto.x86_64");

        List<Configuration> configurations = new ArrayList<>();

        Configuration hadoopEnv = new Configuration()
                .withClassification("hadoop-env")
                .withConfigurations(
                        new Configuration()
                                .withClassification("export")
                                .withProperties(Collections.singletonMap("JAVA_HOME", "/usr/lib/jvm/java-11-amazon-corretto.x86_64"))
                );
        configurations.add(hadoopEnv);

// Add the second configuration object
        Configuration sparkEnv = new Configuration()
                .withClassification("spark-env")
                .withConfigurations(
                        new Configuration()
                                .withClassification("export")
                                .withProperties(Collections.singletonMap("JAVA_HOME", "/usr/lib/jvm/java-11-amazon-corretto.x86_64"))
                );
        configurations.add(sparkEnv);

// Add the third configuration object
        Map<String, String> properties = new HashMap<>();
        properties.put("spark.executorEnv.JAVA_HOME", "/usr/lib/jvm/java-11-amazon-corretto.x86_64");
        Configuration sparkDefaults = new Configuration()
                .withClassification("spark-defaults")
                .withProperties(properties);
        configurations.add(sparkDefaults);

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType("m4.xlarge")
                .withSlaveInstanceType("m4.xlarge")
                .withHadoopVersion("3.3.3")
                .withEc2KeyName("keyPair1")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("cluster from Java WITH COMBINERS - 1")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2)
                .withLogUri("s3://emr-logs-mevuzarot/logs/")
                .withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-6.9.0")
                .withConfigurations(configurations);


        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }

}