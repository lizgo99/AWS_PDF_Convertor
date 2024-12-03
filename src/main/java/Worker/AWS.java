//package Worker;
//
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.ec2.Ec2Client;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.*;
//import software.amazon.awssdk.services.sqs.SqsClient;
//import software.amazon.awssdk.services.sqs.model.*;
//
//import java.io.*;
//
//public class AWS {
//    private final S3Client s3;
//    private final SqsClient sqs;
//
//    public static String ami = "?";
//    public static String WorkerScript = "?";
//
//    public static Region region1 = Region.US_WEST_2;
//    public static Region region2 = Region.US_EAST_1;
//
//    private static final AWS instance = new AWS();
//
//    private AWS() {
//        s3 = S3Client.builder().region(region1).build();
//        sqs = SqsClient.builder().region(region1).build();
//    }
//
//    public static AWS getInstance() {
//        return instance;
//    }
//
//    public String bucketName = "input-bucket-910o31";
//
//    private static boolean DebugOn = true;
//
//    public String uploadFileToS3(String keyPath, File file) throws Exception {
//        System.out.printf("Start upload: %s, to S3\n", file.getName());
//        PutObjectRequest req =
//                PutObjectRequest.builder()
//                        .bucket(bucketName)
//                        .key(keyPath)
//                        .build();
//
//        s3.putObject(req, file.toPath()); // we don't need to check if the file exist already
//
//        // Return the S3 path of the uploaded file
//        return "s3://" + bucketName + "/" + keyPath;
//    }
//
//    public String connectToQueueByName(String queueName) {
//        try {
//            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
//                    .queueName(queueName)
//                    .build();
//            GetQueueUrlResponse getQueueResponse = sqs.getQueueUrl(getQueueRequest);
//            return getQueueResponse.queueUrl();
//        } catch (SqsException e) {
//            System.err.println("Error connecting to queue: " + e.awsErrorDetails().errorMessage());
//            throw e;
//        }
//    }
//
//    public void sendMessageToQueue(String queueUrl, String message) {
//        try {
//            // get queue URL if name was provided instead of URL
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
//            errorMsg("Error sending message: " + e.awsErrorDetails().errorMessage());
//            throw e;
//        }
//    }
//
//    public Message getMessageFromQueue(String queueUrl) {
//        try {
//            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
//                    .queueUrl(queueUrl)
//                    .maxNumberOfMessages(1)
//                    .build();
//
//            ReceiveMessageResponse receiveResponse = sqs.receiveMessage(receiveRequest);
//            if (receiveResponse.messages().size() == 0) {
//                return null;
//            }
//
//            Message message = receiveResponse.messages().get(0);
//            debugMsg("Message received. MessageId: " + message.messageId());
//            return message;
//        } catch (SqsException e) {
//            errorMsg("Error receiving message: " + e.awsErrorDetails().errorMessage());
//            throw e;
//        }
//    }
//
//    public void deleteMessageFromQueue(String queueUrl, String receiptHandle) {
//        try {
//            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
//                    .queueUrl(queueUrl)
//                    .receiptHandle(receiptHandle)
//                    .build();
//
//            sqs.deleteMessage(deleteRequest);
//            debugMsg("Message deleted");
//        } catch (SqsException e) {
//            errorMsg("Error deleting message: " + e.awsErrorDetails().errorMessage());
//            throw e;
//        }
//    }
//
//    // Output functions
//    public static void debugMsg (String msg){
//        if (DebugOn){
//            String blueBold = "\033[1;34m";
//            String reset = "\033[0m";
//            System.out.println(blueBold + "[DEBUG] " + reset + msg);
//        }
//    }
//
//    public static void errorMsg (String msg){
//        String redBold = "\033[1;31m";
//        String reset = "\033[0m";
//        System.out.println(redBold + "[ERROR] " + reset + msg);
//    }
//
//
//}
