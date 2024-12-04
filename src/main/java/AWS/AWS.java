package AWS;

import Manager.Manager;
import Manager.TaskTracker;

import software.amazon.awssdk.core.ResponseInputStream;
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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class AWS {
    //TODO: add as needed
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    private static String ami = "ami-0f3a384f4dd1ea50d";
    private static String ManagerScript = "#!/bin/bash\n" +
            "sudo yum update -y\n" +
            "sudo yum install -y aws-cli\n" +
            "sudo yum install -y java-11-amazon-corretto\n" +
            "sudo wget https://workerjar123.s3.us-west-2.amazonaws.com/Manager.jar -O /home/Manager.jar\n" +
            "java -cp /home/Manager.jar Manager.Manager > /home/manager_output.log 2>&1";

    private static String WorkerScript = "#!/bin/bash\n" +
            "sudo yum update -y\n" +
            "sudo yum install -y aws-cli\n" +
            "sudo yum install -y java-11-amazon-corretto\n" +
            "sudo wget https://workerjar123.s3.us-west-2.amazonaws.com/Worker.jar -O /home/Worker.jar\n" +
            "java -cp /home/Worker.jar Worker.Worker > /home/worker_output.log 2>&1";

    private static Region region1 = Region.US_WEST_2;
    private static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    public String bucketName = "input-bucket-910o311";
    private static boolean DebugOn = true;

    public ConcurrentLinkedQueue<Message> FileTasks = new ConcurrentLinkedQueue<>();
    public ConcurrentHashMap<String, TaskTracker> TasksMap = new ConcurrentHashMap<>();

    public int MAX_WORKERS = 9;
    public AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    //TODO: add as needed
    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
        createBucketIfNotExists(bucketName);
    }
    //TODO: add as needed
    public static AWS getInstance() {
        return instance;
    }

    // S3
    //TODO: LOCAL
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
            debugMsg(e.getMessage());
        }
    }
    //TODO: ALL
    public String uploadFileToS3(String keyPath, File file) throws Exception {
        System.out.printf("Start upload: %s, to S3\n", file.getName());
        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already

        // Return the S3 path of the uploaded file
        return "s3://" + bucketName + "/" + keyPath;
    }
    //TODO: LOCAL, MANAGER
    public BufferedReader downloadFileFromS3(String s3Url) {
        debugMsg("Start downloading file " + s3Url);

        try {
            // Extract the key from the S3 URL
            String key = s3Url.replace("s3://" + bucketName + "/", "");
            
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            ResponseInputStream<GetObjectResponse> s3Object = s3.getObject(getObjectRequest);
            return new BufferedReader(new InputStreamReader(s3Object));
        } catch (Exception e) {
            errorMsg("Error downloading file from S3: " + e.getMessage());
            throw e;
        }
    }
    //TODO: LOCAL
    public void makeFolderPublic(String folderName) {
        try {
            // Create a request to delete the public access block configuration
            DeletePublicAccessBlockRequest request = DeletePublicAccessBlockRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Delete the public access block configuration
            s3.deletePublicAccessBlock(request);

            debugMsg("Successfully removed 'Block all public access' for bucket: " + bucketName);
        } catch (Exception e) {
            errorMsg("Error removing 'Block all public access': " + e.getMessage());
        }

        // JSON Bucket Policy
        String bucketPolicy = "{\n" +
                "    \"Version\": \"2012-10-17\",\n" +
                "    \"Statement\": [\n" +
                "        {\n" +
                "            \"Sid\": \"PublicAccessToFolder\",\n" +
                "            \"Effect\": \"Allow\",\n" +
                "            \"Principal\": \"*\",\n" +
                "            \"Action\": \"s3:GetObject\",\n" +
                "            \"Resource\": \"arn:aws:s3:::" + bucketName + "/" + folderName + "/*\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        try {
            // Create and send the PutBucketPolicyRequest
            PutBucketPolicyRequest policyRequest = PutBucketPolicyRequest.builder()
                    .bucket(bucketName)
                    .policy(bucketPolicy)
                    .build();

            // Apply the policy to the bucket
            s3.putBucketPolicy(policyRequest);

            debugMsg("Bucket policy updated successfully.");
        } catch (Exception e) {
            errorMsg("Error updating bucket policy: " + e.getMessage());
        }
    }
    //TODO: WORKER
    public String getPublicFileUrl(String fileKey) {
        return String.format("https://%s.s3.%s.amazonaws.com/%s", bucketName, region1, fileKey);
    }
    //TODO: LOCAL
    public void emptyBucket(String bucketName) {
        ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
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
            debugMsg("Deleted object: " + objectKey);
        });
        debugMsg("Finished cleaning bucket " + bucketName);
    }
    //TODO: LOCAL
    public void deleteBucket(String bucketName) {
        emptyBucket(bucketName);
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucketName)
                .build();
        s3.deleteBucket(deleteBucketRequest);
        debugMsg("Bucket " + bucketName + " has been deleted");
        s3.close();
    }

    // public boolean doesFileExist(String s3Url) {
    //     try {
    //         // Extract bucket and key from S3 URL
    //         String path = s3Url.replace("s3://" + bucketName + "/", "");

    //         HeadObjectRequest request = HeadObjectRequest.builder()
    //                 .bucket(bucketName)
    //                 .key(path)
    //                 .build();

    //         s3.headObject(request);
    //         return true;
    //     } catch (S3Exception e) {
    //         if (e.statusCode() == 404) {
    //             return false;
    //         }
    //         throw e;
    //     }
    // }

    // EC2
    //TODO: LOCAL, MANAGER
    public String createEC2(String script, String tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();
        RunInstancesRequest runRequest = (RunInstancesRequest) RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(ami)
                .maxCount(numberOfInstances)
                .minCount(1)
                .keyName("vockey")
                .securityGroups("launch-wizard-1")
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .userData(Base64.getEncoder().encodeToString((script).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        // Get all instance IDs
        List<String> instanceIds = response.instances().stream()
                .map(Instance::instanceId)
                .collect(Collectors.toList());

        // Create tag specifications
        Tag nameTag = Tag.builder()
                .key("Name")
                .value(tagName)
                .build();

        // Create tags for all instances at once
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceIds)
                .tags(nameTag)
                .build();

        try {
            ec2.createTags(tagRequest);
            for (String instanceId : instanceIds) {
                System.out.printf(
                    "[DEBUG] Successfully started EC2 instance %s with tag %s\n",
                    instanceId, tagName);
            }
            return instanceIds.get(0); // Return first instance ID for backward compatibility
        } catch (Ec2Exception e) {
            errorMsg("Error creating EC2 instance: " + e.getMessage());
            throw e;
        }
    }

    // SQS
    //TODO: LOCAL, MANAGER
    public String createSqsQueue(String queueName) {
        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
            String queueUrl = createQueueResponse.queueUrl();
            debugMsg("Created queue. URL: " + queueUrl);
            return queueUrl;
        } catch (SqsException e) {
            errorMsg("Error creating queue: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }
    //TODO: ALL
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
            debugMsg("Message sent. MessageId: " + sendResponse.messageId());
        } catch (SqsException e) {
            errorMsg("Error sending message: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }
    //TODO: LOCAL, MANAGER
    public void deleteQueue(String queueUrl) {
        try {
            DeleteQueueRequest deleteRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteRequest);
            debugMsg("Queue deleted successfully: " + queueUrl);
        } catch (Exception e) {
            errorMsg("Error deleting the queue (" + queueUrl + "):" + e.getMessage());
        } finally {
            sqs.close();
        }
    }

    // SQS: Worker Additions
    //TODO: WORKER
    public Message getMessageFromQueue(String queueUrl) {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .visibilityTimeout(30)  // ? is this a good number?
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
    //TODO: LOCAL, MANAGER
    public List<Message> pollMessages(String queueUrl) {
        try {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10) // what if there are more than 10 messages?
                    .build();

            ReceiveMessageResponse receiveResponse = sqs.receiveMessage(receiveRequest);
            if (receiveResponse.messages().isEmpty()) {
                return null;
            }

            List<Message> messages = receiveResponse.messages();
            debugMsg("Messages received. Number of messages: " + messages.size());
            return messages;
        } catch (SqsException e) {
            errorMsg("Error receiving messages: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }
    //TODO: ALL
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
    //TODO: MANAGER, WORKER
    public String connectToQueueByName(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            GetQueueUrlResponse getQueueResponse = sqs.getQueueUrl(getQueueRequest);
            return getQueueResponse.queueUrl();
        } catch (SqsException e) {
            errorMsg("Error connecting to queue: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }
    //TODO: LOCAL
    public void startManagerIfNotActive() {
        // Check if any instances were found
        if (!isManagerActive()) {
            createEC2(ManagerScript,  Label.Manager.name(), 1);
            debugMsg("LocalApp created a Manager EC2 instance");
        }
    }
    //TODO: LOCAL
    public boolean isManagerActive() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if ((instance.state().name() == InstanceStateName.RUNNING ||
                            instance.state().name() == InstanceStateName.PENDING) &&
                            tag.key().equals("Name") && tag.value().equals("Manager")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Output functions
    //TODO: ALL
    public static void debugMsg(String msg) {
        if (DebugOn) {
            String blueBold = "\033[1;34m";
            String reset = "\033[0m";
            System.out.println(blueBold + "[DEBUG] " + reset + msg);
        }
    }
    //TODO: ALL
    public static void errorMsg(String msg) {
        String redBold = "\033[1;31m";
        String reset = "\033[0m";
        System.out.println(redBold + "[ERROR] " + reset + msg);
    }
    //TODO: MANAGER
    public int getQueueMessageCount(String queueUrl) {
        try {
            GetQueueAttributesResponse response = sqs.getQueueAttributes(builder -> builder
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
            return Integer.parseInt(response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
        } catch (Exception e) {
            errorMsg("Error getting queue message count: " + e.getMessage());
            return 0;
        }
    }
    //TODO: MANAGER
    public void startWorkerInstances(int numberOfInstances) {
        try {
            createEC2(WorkerScript, "Worker", numberOfInstances);
            debugMsg("Started " + numberOfInstances + " worker instances");
        } catch (Exception e) {
            errorMsg("Failed to start worker instance: " + e.getMessage());
        }
    }
    //TODO: MANAGER
    public void terminateAllWorkerInstances() {
        try {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    for (Tag tag : instance.tags()) {
                        if (tag.key().equals("Name") && tag.value().equals(Label.Worker.name()) &&
                                (instance.state().name() == InstanceStateName.RUNNING ||
                                instance.state().name() == InstanceStateName.PENDING)) {
                            terminateInstance(instance.instanceId());
                        }
                    }
                }
            }
        } catch (Exception e) {
            errorMsg("Failed to terminate worker instances: " + e.getMessage());
        }
    }
    //TODO: MANAGER
    private void terminateInstance(String instanceId) {
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            ec2.terminateInstances(request);
            debugMsg("Instance terminated: " + instanceId);
        } catch (Exception e) {
            errorMsg("Failed to terminate instance " + instanceId + ": " + e.getMessage());
        }
    }
    //TODO: MANAGER, WORKER
    public void changeVisibilityTimeout(String queueUrl, String receiptHandle, int timeout) {
        ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .visibilityTimeout(timeout)
                .build();
        sqs.changeMessageVisibility(request);
        debugMsg("Visibility timeout changed for message " + receiptHandle);
    }

//    public void setTerminate(boolean terminate) {
//        this.shouldTerminate.set(terminate);
//    }
//
//    public List<Instance> getAllInstances() {
//        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder().build();
//        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);
//        ;
//        return describeInstancesResponse.reservations().stream()
//                .flatMap(r -> r.instances().stream())
//                .collect(Collectors.toList());
//    }
//
//    public List<Instance> getAllInstancesWithLabel(Label label) throws InterruptedException {
//        DescribeInstancesRequest describeInstancesRequest = DescribeInstancesRequest.builder()
//                .filters(Filter.builder()
//                        .name("tag:Label")
//                        .values(label.toString())
//                        .build())
//                .build();
//
//        DescribeInstancesResponse describeInstancesResponse = ec2.describeInstances(describeInstancesRequest);
//
//        return describeInstancesResponse.reservations().stream()
//                .flatMap(r -> r.instances().stream())
//                .collect(Collectors.toList());
//    }
    //TODO: LOCAL, MANAGER
    public enum Label {
        Manager,
        Worker
    }
    //TODO: MANAGER
    public void terminateManagerInstance() {
        try {
            terminateAllWorkerInstances();
            Thread.sleep(5000);

            DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
            DescribeInstancesResponse response = ec2.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    for (Tag tag : instance.tags()) {
                        if (tag.key().equals("Name") && tag.value().equals(Label.Manager.name()) &&
                            (instance.state().name() == InstanceStateName.RUNNING ||
                             instance.state().name() == InstanceStateName.PENDING)) {
                            terminateInstance(instance.instanceId());
                        }
                    }
                }
            }
        } catch (Exception e) {
            errorMsg("Failed to terminate manager instance: " + e.getMessage());
        }
    }
    //TODO: MANAGER
    public void cleanup() {
        try {
            // First terminate all instances
            terminateAllWorkerInstances();
            terminateManagerInstance();
            
            // Keep clients open until the very end
            try {
                Thread.sleep(5000); // Wait for instance termination to complete
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Finally close the clients
            try {
                ec2.close();
                s3.close();
                sqs.close();
            } catch (Exception e) {
                errorMsg("Error closing AWS clients: " + e.getMessage());
            }
        } catch (Exception e) {
            errorMsg("Error during cleanup: " + e.getMessage());
        }
    }
}
