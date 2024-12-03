package Manager;

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
//    public void downloadFileFromS3(String keyPath, File outputFile) {
//        System.out.println("Start downloading file " + keyPath + " to " + outputFile.getPath());
//
//        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
//                .bucket(bucketName)
//                .key(keyPath)
//                .build();
//
//        try {
//            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(getObjectRequest);
//            byte[] data = objectBytes.asByteArray();
//
//            // Write the data to a local file.
//            OutputStream os = new FileOutputStream(outputFile);
//            os.write(data);
//            System.out.println("Successfully obtained bytes from an S3 object");
//            os.close();
//        } catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
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

}
