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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import AWS.AWS;

public class LocalApp {

    final static AWS aws = AWS.getInstance();
    private static String RESULT_QUEUE_URL;
    public static String inputFileName = "test-samples.txt";

    public static HashSet<String> QueueUrls;

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

        File inputFile = new File("test-samples.txt");

        // Create manager if one doesn't exist
        aws.createManagerIfDoesNotExist();

        // Create a bucket and upload the file
        aws.createBucketIfNotExists(aws.bucketName);
        String fileLocation =  aws.uploadFileToS3(inputFile.getName(), inputFile);

        // Create a new SQS queue
        String queueName = "LocalAppToManagerQueue";
        String queueUrl = aws.createSqsQueue(queueName);
        QueueUrls.add(queueUrl);

        // Upload file location to the SQS
        aws.sendMessageToQueue(queueUrl, fileLocation);

        RESULT_QUEUE_URL = aws.createSqsQueue("ResultQueue");
        QueueUrls.add(queueUrl);

        String summeryURL = waitForSummaryFile();

        if (summeryURL != null) {
            File summeryFile = new File("summery.txt");
            aws.downloadFileFromS3(summeryURL, summeryFile);
            File outputFile = new File("output.txt");
            createHTMLFile(summeryFile.getPath(), outputFile.getPath());
            System.out.println("outputFile: " + outputFile.getPath());
        }

        cleanup();




    }

    public static String waitForSummaryFile() {

        String summaryFileUrl = null;

        try (SqsClient sqsClient = SqsClient.create()) {
            System.out.println("Waiting for summary file...");

            boolean summaryReceived = false;

            while (!summaryReceived) {
                // Poll the ResultQueue for a message
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(RESULT_QUEUE_URL)
                        .maxNumberOfMessages(1) // Fetch one message at a time
                        .waitTimeSeconds(5)    // Long polling for 5 seconds
                        .build();

                List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

                for (Message message : messages) {
                    // Process the message
                    System.out.println("Received message: " + message.body());

                    // Assuming the message contains the summary file URL in JSON format
                    summaryFileUrl = extractSummaryFileUrl(message.body());
                    System.out.println("Summary file is available at: " + summaryFileUrl);

                    // Mark the summary as received and exit loop
                    summaryReceived = true;

                    // Delete the processed message from the queue
                    sqsClient.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(RESULT_QUEUE_URL)
                            .receiptHandle(message.receiptHandle())
                            .build());
                    break; // Exit the loop after processing a valid message
                }
            }
        } catch (Exception e) {
            System.err.println("Error waiting for summary file: " + e.getMessage());
        }
        return summaryFileUrl;
    }

    // Helper method to extract the summary file URL from the message body
    private static String extractSummaryFileUrl(String messageBody) {
        // Assume the message body is in JSON format: {"summaryFileUrl": "s3://my-bucket/summary.html"}
        // Parse it to extract the S3 URL (use a JSON library if needed)
        if (messageBody.contains("summaryFileUrl")) {
            return messageBody.split(":")[1].replace("}", "").replace("\"", "").trim();
        }
        return "Unknown";
    }

    public static void createHTMLFile(String summaryFilePath, String outputHtmlPath) {
        StringBuilder htmlContent = new StringBuilder();
        htmlContent.append("<!DOCTYPE html>\n<html>\n<head>\n<title>PDF Processing Summary</title>\n</head>\n<body>\n");
        htmlContent.append("<h1>PDF Processing Summary</h1>\n");
        htmlContent.append("<table border=\"1\">\n<tr><th>Operation</th><th>Input File</th><th>Result</th></tr>\n");

        try (BufferedReader reader = new BufferedReader(new FileReader(summaryFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 3); // Split into operation, input, and result
                if (parts.length < 3) continue; // Skip malformed lines
                String operation = parts[0];
                String inputUrl = parts[1];
                String result = parts[2];

                htmlContent.append("<tr>");
                htmlContent.append("<td>").append(operation).append("</td>");
                htmlContent.append("<td><a href=\"").append(inputUrl).append("\">").append(inputUrl).append("</a></td>");
                if (result.startsWith("Operation failed")) {
                    htmlContent.append("<td>").append(result).append("</td>");
                } else {
                    htmlContent.append("<td><a href=\"").append(result).append("\">Output File</a></td>");
                }
                htmlContent.append("</tr>\n");
            }
        } catch (Exception e) {
            System.err.println("Error reading summary file: " + e.getMessage());
        }

        htmlContent.append("</table>\n</body>\n</html>");

        try (FileWriter writer = new FileWriter(outputHtmlPath)) {
            writer.write(htmlContent.toString());
            System.out.println("HTML file created: " + outputHtmlPath);
        } catch (Exception e) {
            System.err.println("Error writing HTML file: " + e.getMessage());
        }
    }

    // TODO: Create a cleanup function to delete all the buckets and queues
    public static void cleanup(){
        // Delete all the queues
        for (String queueUrl : QueueUrls){
            aws.deleteQueue(queueUrl);
            QueueUrls.remove(queueUrl);
        }
        // Delete all the buckets - files inside the bucket
        aws.deleteBucket(aws.bucketName);
    }
}
