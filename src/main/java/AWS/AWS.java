package AWS;

import jdk.internal.org.jline.terminal.TerminalBuilder;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.util.Base64;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;



    public static String ami = "ami-00e95a9222311e8ed";
    public static String ManagerScript = "#!/bin/bash\n" +
            "yum update -y\n" +
            "yum install -y aws-cli\n" +
            "aws s3 cp s3://bucket15032000/Manager.jar /home/Manager.jar\n" +
            "java -cp /home/Manager.jar  Manager\n";


    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region1).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    public String bucketName = "input-bucket-910o31";
    private static boolean DebugOn = true;

    // S3
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public String uploadFileToS3(String keyPath, File file) throws Exception {
        System.out.printf("Start upload: %s, to S3\n", file.getName());
        PutObjectRequest req =
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyPath)
                        .build();

        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already

        // Return the S3 path of the uploaded file
        return "s3://" + bucketName + "/" + keyPath;
    }

    public void downloadFileFromS3(String keyPath, File outputFile) {
        System.out.println("Start downloading file " + keyPath + " to " + outputFile.getPath());

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        try {
            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file.
            OutputStream os = new FileOutputStream(outputFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteBucket(String bucketName){
        emptyBucket(bucketName);
        DeleteBucketRequest deleteBucketRequest =
                DeleteBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
        s3.deleteBucket(deleteBucketRequest);
        debugMsg("Bucket " + bucketName + " has been deleted");
        s3.close();
    }

    public void emptyBucket(String bucketName){
        ListObjectsRequest listObjectsRequest =
                ListObjectsRequest.builder()
                        .bucket(bucketName)
                        .build();

        ListObjectsResponse listObjectsResponse = s3.listObjects(listObjectsRequest);

        debugMsg("Starting to clean bucket " + bucketName);
        listObjectsResponse.contents().forEach(s3Object -> {
            String objectKey = s3Object.key();
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();
            s3.deleteObject(deleteRequest);
            System.out.println("Deleted object: " + objectKey);
        });
        debugMsg("Finished cleaning bucket " + bucketName);
    }

    // EC2
    public String createEC2(String script, String tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region1).build();
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();


        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        CreateTagsRequest tagRequest = (CreateTagsRequest) CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
                    instanceId, ami);

        } catch (Ec2Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            System.exit(1);
        }
        return instanceId;
    }

    // SQS
    public String createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
        String queueUrl = createQueueResponse.queueUrl();
        System.out.println("Created queue. URL: " + queueUrl);
        return queueUrl;

    }

    public void sendMessageToQueue(String queueUrl, String message) {
        try {
            // get queue URL if name was provided instead of URL
            if (!queueUrl.startsWith("https://")) {
                GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                        .queueName(queueUrl)
                        .build();
                queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
            }

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build();

            SendMessageResponse sendResponse = sqs.sendMessage(sendMsgRequest);
            System.out.println("Message sent. MessageId: " + sendResponse.messageId());
        } catch (SqsException e) {
            errorMsg("Error sending message: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public void deleteQueue(String queueUrl) {
        try {
            DeleteQueueRequest deleteRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqsClient.deleteQueue(deleteRequest);
            debugMsg("Queue deleted successfully: " + queueUrl);
        } catch (Exception e) {
            errorMsg("Error deleting the queue (" + queueUrl + "):" + e.getMessage());
        } finally {
            sqsClient.close();
        }
    }

    // SQS: Worker Additions

    public Message getMessageFromQueue(String queueUrl) {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build();

            ReceiveMessageResponse receiveResponse = sqs.receiveMessage(receiveRequest);
            if (receiveResponse.messages().size() == 0) {
                return null;
            }

            Message message = receiveResponse.messages().get(0);
            debugMsg("Message received. MessageId: " + message.messageId());
            return message;
        } catch (SqsException e) {
            errorMsg("Error receiving message: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public void deleteMessageFromQueue(String queueUrl, String receiptHandle) {
        try {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();

            sqs.deleteMessage(deleteRequest);
            debugMsg("Message deleted");
        } catch (SqsException e) {
            errorMsg("Error deleting message: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public String connectToQueueByName(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            GetQueueUrlResponse getQueueResponse = sqs.getQueueUrl(getQueueRequest);
            return getQueueResponse.queueUrl();
        } catch (SqsException e) {
            System.err.println("Error connecting to queue: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public void startManagerIfNotActive() {

        // // Describe instances with the specified tag
        // DescribeInstancesResponse response = ec2.describeInstances(
        //         DescribeInstancesRequest.builder().filters(
        //                 Filter.builder()
        //                         .name("tag:" + "Name")
        //                         .values("Manager")
        //                         .build()
        //         ).build()
        // );

        // Check if any instances were found
        if(!isManagerActive()){
            createEC2(ManagerScript, "Manager", 1);
            System.out.print("[DEBUG] LocalApp - Manager created\n");
        }
    }

    public boolean isManagerActive(){
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                for(Tag tag : instance.tags()){
                    if((instance.state().name()==InstanceStateName.RUNNING ||
                            instance.state().name()==InstanceStateName.PENDING) &&
                            tag.key().equals("Name") && tag.value().equals("MANAGER")){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Output functions
    public static void debugMsg (String msg){
        if (DebugOn){
            String blueBold = "\033[1;34m";
            String reset = "\033[0m";
            System.out.println(blueBold + "[DEBUG] " + reset + msg);
        }
    }

    public static void errorMsg (String msg){
        String redBold = "\033[1;31m";
        String reset = "\033[0m";
        System.out.println(redBold + "[ERROR] " + reset + msg);
    }

}

