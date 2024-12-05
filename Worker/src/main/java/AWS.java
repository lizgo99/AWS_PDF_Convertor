package Worker.src.main.java;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;

public class AWS {
    ////////////////// Fields //////////////////

    private final S3Client s3;
    private final Ec2Client ec2;
    private final SqsClient sqs;

    private static final Region region1 = Region.US_WEST_2;
    private static final Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private static final String ami = "ami-0f3a384f4dd1ea50d";

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
    public String uploadFileToS3(String bucketName, String keyPath, File file){
        debugMsg("Start upload: %s, to S3", file.getName());
        PutObjectRequest req = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyPath)
                .build();

        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already

        // Return the S3 path of the uploaded file
        return "s3://" + bucketName + "/" + keyPath;
    }

    public String getPublicFileUrl(String bucketName, String fileKey) {
        return String.format("https://%s.s3.%s.amazonaws.com/%s", bucketName, region1, fileKey);
    }

    ////////////////// SQS //////////////////
    public String connectToQueueByName(String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            GetQueueUrlResponse getQueueResponse = sqs.getQueueUrl(getQueueRequest);
            return getQueueResponse.queueUrl();
        } catch (SqsException e) {
            errorMsg("Error connecting to queue: %s" , e.awsErrorDetails().errorMessage());
            throw e;
        }
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
            debugMsg("Message sent. MessageId: %s" , sendResponse.messageId());
        } catch (SqsException e) {
            errorMsg("Error sending message: %s" , e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

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
            debugMsg("Message received. MessageId: %s" , message.messageId());
            return message;
        } catch (SqsException e) {
            errorMsg("Error receiving message: %s" , e.awsErrorDetails().errorMessage());
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
            errorMsg("Error deleting message: %s" , e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public void changeVisibilityTimeout(String queueUrl, String receiptHandle, int timeout) {
        ChangeMessageVisibilityRequest request = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .visibilityTimeout(timeout)
                .build();
        sqs.changeMessageVisibility(request);
        debugMsg("Visibility timeout changed for message %s" , receiptHandle);
    }

    ////////////////// MESSAGE HANDLERS //////////////////
    public static void changeDebugMode(){
        DEBUG = !DEBUG;
    }

    public static void debugMsg(String format, Object... args) {
        if (DEBUG) {
            String blueBold = "\033[1;34m";
            String reset = "\033[0m";
            String formattedMsg = String.format(format, args);
            System.out.println(blueBold + "[DEBUG] " + reset + formattedMsg);
        }
    }

    public static void errorMsg(String format, Object... args) {
        String redBold = "\033[1;31m";
        String reset = "\033[0m";
        String formattedMsg = String.format(format, args);
        System.out.println(redBold + "[ERROR] " + reset + formattedMsg);
    }
}
