import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

//import java.io.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class AWS {
    ////////////////// Fields //////////////////

    private final S3Client s3;
    private final Ec2Client ec2;
    private final SqsClient sqs;

    private static final Region region1 = Region.US_WEST_2;
    private static final Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private static final String ami = "ami-0f3a384f4dd1ea50d";
    private static final String WorkerScript = "#!/bin/bash\n" +
            "sudo yum update -y\n" +
            "sudo yum install -y aws-cli\n" +
            "sudo yum install -y java-11-amazon-corretto\n" +
            "sudo wget https://workerjar123.s3.us-west-2.amazonaws.com/resources/Worker.jar -O /home/Worker.jar\n" +
//            "sudo wget https://bucketforjars.s3.us-west-2.amazonaws.com/Worker.jar -O /home/Worker.jar\n" +
            "java -cp /home/Worker.jar Worker > /home/worker_output.log 2>&1";

    private static boolean DEBUG = true;

    public enum Label {
        Manager,
        Worker
    }

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
        sqs = SqsClient.builder().region(region1).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    ////////////////// S3 //////////////////
    public String uploadFileToS3(String bucketName, String keyPath, File file) {
        debugMsg("Start upload: %s, to S3", file.getName());
        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already

        // Return the S3 path of the uploaded file
        return "s3://" + bucketName + "/" + keyPath;
    }

    public BufferedReader downloadFileFromS3(String bucketName, String s3Url) {
        debugMsg("Start downloading file: %s", s3Url);

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
            errorMsg("Error downloading file from S3: %s", e.getMessage());
            throw e;
        }
    }

    ////////////////// EC2 //////////////////
    public String createEC2(String script, Label tagName, int numberOfInstances) {
        Ec2Client ec2 = Ec2Client.builder().region(region2).build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
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
                .value(tagName.name())
                .build();

        // Create tags for all instances at once
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceIds)
                .tags(nameTag)
                .build();

        try {
            ec2.createTags(tagRequest);
            for (String instanceId : instanceIds) {
                debugMsg("Successfully started EC2 instance %s with tag %s",
                        instanceId, tagName);
            }
            return instanceIds.get(0); // Return first instance ID for backward compatibility
        } catch (Ec2Exception e) {
            errorMsg("Error creating EC2 instance: %s", e.getMessage());
            throw e;
        }
    }

    public void startWorkerInstances(int numberOfInstances) {
        try {
            createEC2(WorkerScript, Label.Worker, numberOfInstances);
            debugMsg("Started %s  worker instances", numberOfInstances);
        } catch (Exception e) {
            errorMsg("Failed to start worker instance: %s", e.getMessage());
        }
    }

    ////////////////// SQS //////////////////
    public String createSqsQueue(String queueName) {
        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);
            String queueUrl = createQueueResponse.queueUrl();
            debugMsg("Created queue. URL: %s", queueUrl);
            return queueUrl;
        } catch (SqsException e) {
            errorMsg("Error creating queue: %s", e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public void sendMessageToQueue(String queueUrl, String message) {
        try {
            // get queue URL if name was provided instead of URL
            if (!queueUrl.startsWith("https://")) {
                queueUrl = connectToQueueByName(queueUrl);
            }

            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build();

            SendMessageResponse sendResponse = sqs.sendMessage(sendMsgRequest);
            debugMsg("Message sent. MessageId: %s", sendResponse.messageId());
            debugMsg("The message is: %s", message);
        } catch (SqsException e) {
            errorMsg("Error sending message: %s", e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public void deleteQueue(String queueUrl) {
        try {
            // get queue URL if name was provided instead of URL
            if (!queueUrl.startsWith("https://")) {
                queueUrl = connectToQueueByName(queueUrl);
            }

            DeleteQueueRequest deleteRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqs.deleteQueue(deleteRequest);
            debugMsg("Queue deleted successfully: %s" , queueUrl);
        } catch (Exception e) {
            errorMsg("Error deleting the queue (%s): %s", queueUrl, e.getMessage());
        }
    }

    public List<Message> pollMessages(String queueUrl) {
        try {
            // get queue URL if name was provided instead of URL
            if (!queueUrl.startsWith("https://")) {
                queueUrl = connectToQueueByName(queueUrl);
            }
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .visibilityTimeout(5)
                    .build();

            ReceiveMessageResponse receiveResponse = sqs.receiveMessage(receiveRequest);
            if (receiveResponse.messages().isEmpty()) {
                return null;
            }

            List<Message> messages = receiveResponse.messages();
            debugMsg("Messages received. Number of messages: %s" , messages.size());
            return messages;
        } catch (SqsException e) {
            errorMsg("Error receiving messages: %s" , e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public void deleteMessageFromQueue(String queueUrl, String receiptHandle) {
        try {
            // get queue URL if name was provided instead of URL
            if (!queueUrl.startsWith("https://")) {
                queueUrl = connectToQueueByName(queueUrl);
            }
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(receiptHandle)
                    .build();

            sqs.deleteMessage(deleteRequest);
            debugMsg("Message deleted");
        } catch (SqsException e) {
            errorMsg("Error deleting message: %s", e.awsErrorDetails().errorMessage());
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
            errorMsg("Error connecting to queue: %s", e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public int getQueueMessageCount(String queueUrl) {
        try {
            // get queue URL if name was provided instead of URL
            if (!queueUrl.startsWith("https://")) {
                queueUrl = connectToQueueByName(queueUrl);
            }

            GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                    .build();
            
            GetQueueAttributesResponse response = sqs.getQueueAttributes(request);
            return Integer.parseInt(response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES));
        } catch (Exception e) {
            errorMsg("Error getting queue message count: %s", e.getMessage());
            return 0;
        }
    }

    public void changeVisibilityTimeout(String queueUrl, String receiptHandle, int timeout) {
        // get queue URL if name was provided instead of URL
        if (!queueUrl.startsWith("https://")) {
            queueUrl = connectToQueueByName(queueUrl);
        }
        ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .visibilityTimeout(timeout)
                .build();
        sqs.changeMessageVisibility(request);
        debugMsg("Visibility timeout changed for message %s", receiptHandle);
    }

    ////////////////// TERMINATION //////////////////
    public void terminateAllRunningWorkerInstances() {
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
            errorMsg("Failed to terminate worker instances: %s", e.getMessage());
        }
    }

    private void terminateInstance(String instanceId) {
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();
            ec2.terminateInstances(request);
            debugMsg("Instance terminated: %s", instanceId);
        } catch (Exception e) {
            errorMsg("Failed to terminate instance %s: %s", instanceId, e.getMessage());
        }
    }

    public void terminateManagerInstance() {
        try {
            terminateAllRunningWorkerInstances();
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
            errorMsg("Failed to terminate manager instance: %s", e.getMessage());
        }
    }

    public void cleanup() {
        try {
            // First terminate all instances
            terminateAllRunningWorkerInstances();

            // Wait briefly for termination to complete
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Then terminate the manager
            terminateManagerInstance();

            // Wait for termination to complete
            try {
                Thread.sleep(50000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Finally close the clients
            closeClients();
            
        } catch (Exception e) {
            errorMsg("Error during cleanup: %s", e.getMessage());
        }
    }

    private void closeClients() {
        try {
            if (ec2 != null) {
                ec2.close();
                debugMsg("EC2 client closed successfully");
            }
            if (s3 != null) {
                s3.close();
                debugMsg("S3 client closed successfully");
            }
            if (sqs != null) {
                sqs.close();
                debugMsg("SQS client closed successfully");
            }
        } catch (Exception e) {
            errorMsg("Error closing AWS clients: %s", e.getMessage());
        }
    }
    
    

    ////////////////// MESSAGE HANDLERS //////////////////
    public static void changeDebugMode() {
        DEBUG = !DEBUG;
    }

    public static void debugMsg(String format, Object... args) {
        if (DEBUG) {
            String blueBold = "\033[1;34m";
            String reset = "\033[0m";
            String formattedMsg = String.format(format, args);
            System.out.println(blueBold + "[DEBUG] " + reset + "Manager: " + formattedMsg);
        }
    }

    public static void errorMsg(String format, Object... args) {
        String redBold = "\033[1;31m";
        String reset = "\033[0m";
        String formattedMsg = String.format(format, args);
        System.out.println(redBold + "[ERROR] " + reset + "Manager: " + formattedMsg);
    }

}