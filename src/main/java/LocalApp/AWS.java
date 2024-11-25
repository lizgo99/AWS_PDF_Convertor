package LocalApp;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.util.Base64;

public class AWS {
//    private final S3Client s3;
//    private final SqsClient sqs;
//    private final Ec2Client ec2;
//
//    public static String ami = "ami-00e95a9222311e8ed";
//
//    public static Region region1 = Region.US_WEST_2;
//    public static Region region2 = Region.US_EAST_1;
//
//    private static final AWS instance = new AWS();
//
//    private AWS() {
//        s3 = S3Client.builder().region(region1).build();
//        sqs = SqsClient.builder().region(region1).build();
//        ec2 = Ec2Client.builder().region(region1).build();
//    }
//
//    public static AWS getInstance() {
//        return instance;
//    }
//
//    public String bucketName = "input-bucket-910o3";
//
//
//    // S3
//    public void createBucketIfNotExists(String bucketName) {
//        try {
//            s3.createBucket(CreateBucketRequest
//                    .builder()
//                    .bucket(bucketName)
//                    .createBucketConfiguration(
//                            CreateBucketConfiguration.builder()
//                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
//                                    .build())
//                    .build());
//            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
//                    .bucket(bucketName)
//                    .build());
//        } catch (S3Exception e) {
//            System.out.println(e.getMessage());
//        }
//    }
//
//    public String uploadFileToS3(String keyPath, File file) throws Exception {
//        System.out.printf("Start upload: %s, to S3\n", file.getName());
//        PutObjectRequest req =
//                PutObjectRequest.builder()
//                        .bucket(bucketName)
//                        .key(keyPath)
//                        .build();
//
//        this.s3.putObject(req, file.toPath()); // we don't need to check if the file exist already
//
//        // Return the S3 path of the uploaded file
//        return "s3://" + bucketName + "/" + keyPath;
//    }
//
//    // EC2
//    public String createEC2(String script, String tagName, int numberOfInstances) {
//        Ec2Client ec2 = Ec2Client.builder().region(region2).build();
//        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
//                .instanceType(InstanceType.M4_LARGE)
//                .imageId(ami)
//                .maxCount(numberOfInstances)
//                .minCount(1)
//                .keyName("vockey")
//                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
//                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
//                .build();
//
//
//        RunInstancesResponse response = ec2.runInstances(runRequest);
//
//        String instanceId = response.instances().get(0).instanceId();
//
//        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
//                .key("Name")
//                .value(tagName)
//                .build();
//
//        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
//                .resources(instanceId)
//                .tags(tag)
//                .build();
//
//        try {
//            ec2.createTags(tagRequest);
//            System.out.printf(
//                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
//                    instanceId, ami);
//
//        } catch (Ec2Exception e) {
//            System.err.println("[ERROR] " + e.getMessage());
//            System.exit(1);
//        }
//        return instanceId;
//    }
//
//    public String createSqsQueue(String queueName) {
//        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
//                .queueName(queueName)
//                .build();
//
//        CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
//        String queueUrl = createQueueResponse.queueUrl();
//        System.out.println("Created queue. URL: " + queueUrl);
//        return queueUrl;
//
//    }
//
//    public void sendMessageToQueue(String queueUrl, String message) {
//        try {
//            // First, get queue URL if name was provided instead of URL
//            if (!queueUrl.startsWith("https://")) {
//                GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
//                        .queueName(queueUrl)
//                        .build();
//                queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
//            }
//
//            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
//                    .queueUrl(queueUrl)
//                    .messageBody(message)
//                    .build();
//
//            SendMessageResponse sendResponse = sqs.sendMessage(sendMsgRequest);
//            System.out.println("Message sent. MessageId: " + sendResponse.messageId());
//        } catch (SqsException e) {
//            System.err.println("Error sending message: " + e.awsErrorDetails().errorMessage());
//            throw e;
//        }
//    }
//
//
////    public void sendMessageToQueue(String queueUrl, String s3FileUrl) {
////
////        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
////                .queueUrl(queueUrl)
////                .messageBody("File uploaded to S3 at: " + s3FileUrl)
////                .build();
////
////        // Send the message
////        SendMessageResponse sendResponse = sqs.sendMessage(sendMsgRequest);
////        System.out.println("Message sent to SQS. Message ID: " + sendResponse.messageId());
////
////    }



}

