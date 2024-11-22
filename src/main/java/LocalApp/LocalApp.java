package LocalApp;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalApp {

    final static AWS aws = AWS.getInstance();
    public static String inputFileName = "input-sample-2.txt";

    public static void main(String[] args) throws Exception {
        //read from terminal >java -jar yourjar.jar inputFileName outputFileName n [terminate]
//        if (args.length < 3) {
//            System.err.println("Usage: java -jar LocalApp.jar inputFileName outputFileName n [terminate]");
//            System.exit(1);
//        }
//
//        String inputFileName = args[0];
//        System.out.println("inputFileName: " + inputFileName);
//        String outputFileName = args[1];
//        System.out.println("outputFileName: " + outputFileName);
//        int n = Integer.parseInt(args[2]);
//        boolean terminate = args.length > 3 && args[3].equals("terminate");


        File inputFile = new File("input-sample-2.txt");

        aws.createBucketIfNotExists(aws.bucketName);
        aws.uploadFileToS3(inputFileName, inputFile);

        // Create a new SQS queue
        String queueName = "LocalAppToManagerQueue";
        String queueUrl = aws.createSqsQueue(queueName);
        aws.sendMessageToQueue(queueUrl, inputFileName);







    }
    // TODO: Create a cleanup function to delete all the buckets and queues
    public void cleanup(){
        // Delete all the queues
        // Delete all the buckets - files inside the bucket
    }
}
