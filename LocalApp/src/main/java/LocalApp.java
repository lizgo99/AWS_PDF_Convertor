//import software.amazon.awssdk.core.ResponseBytes;
//import software.amazon.awssdk.core.exception.AbortedException;
//import software.amazon.awssdk.core.pagination.sync.SdkIterable;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.ec2.Ec2Client;
//import software.amazon.awssdk.services.ec2.model.Tag;
//import software.amazon.awssdk.services.ec2.model.*;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.*;
//import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
//import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
//import java.nio.charset.StandardCharsets;
import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;


public class LocalApp {

    final static AWS aws = AWS.getInstance();
    private static String RESULT_QUEUE_URL = null;
//    public static String inputFileName = "input-sample-2.txt";
    private static final String ID = generateRandomID("");
    public static HashSet<String> QueueUrls = new HashSet<>();
    public static String localAppToManager = "LocalAppToManager";
    public static String ManagerToLocalApp = "ManagerToLocalApp";

    public static void main(String[] args) throws Exception {
        //read from terminal >java -jar yourjar.jar inputFileName outputFileName n [terminate]
        if (args.length < 3) {
            AWS.errorMsg("Usage: java -jar LocalApp.jar inputFileName outputFileName n [terminate]");
            System.exit(1);
        }

        String inputFileName = args[0];
        AWS.debugMsg("inputFileName: %s", inputFileName);
        String outputFileName = args[1];
        AWS.debugMsg("outputFileName: %s", outputFileName);
        int pdfsPerWorker = Integer.parseInt(args[2]);
        boolean terminate = args.length > 3 && args[3].equals("terminate");
        File inputFile = new File(inputFileName);

        // Create manager if one doesn't exist
        aws.startManagerIfNotActive();

        // Create a bucket and upload the file
        aws.createBucketIfNotExists(ID);
        String fileLocation =  aws.uploadFileToS3(ID, "inputs/" + inputFile.getName(), inputFile);

        // Create a new SQS queue
        String queueName = "LocalAppToManager";
        String queueUrl = aws.createSqsQueue(queueName);
        QueueUrls.add(queueUrl);

        // Upload file location to the SQS
        AWS.debugMsg("fileLocation: %s", fileLocation);
        aws.sendMessageToQueue(queueUrl, fileLocation + "\t" + pdfsPerWorker);

        RESULT_QUEUE_URL = aws.createSqsQueue(ManagerToLocalApp);
        QueueUrls.add(queueUrl);

        String summeryURL = waitForSummaryFile();

        if (summeryURL != null) {
            File summeryFile = new File("summery.txt");try (BufferedReader output = aws.downloadFileFromS3(ID, summeryURL); FileWriter writer = new FileWriter(summeryFile)) {
                String line;
                while ((line = output.readLine()) != null) {
                    writer.write(line + "\n");
                }
                writer.flush();
                File outputFile = new File(outputFileName);

                createHTMLFile(summeryFile.getPath(), outputFile.getPath());

                AWS.debugMsg("outputFile: %s", outputFile.getPath());
            }
        }

        aws.makeFolderPublic(ID,"outputs");

//        cleanup();
        if (terminate){
            aws.sendMessageToQueue(localAppToManager, "terminate");
        }




    }

    public static String waitForSummaryFile() {

        String summaryFileUrl = null;
        long startTime = System.currentTimeMillis();
        try {
            AWS.debugMsg("Waiting for summary file...");

            boolean summaryReceived = false;

            while (!summaryReceived) {
                // Poll the ResultQueue for a message

                List<Message> messages = aws.pollMessages(RESULT_QUEUE_URL);
                if (messages != null) {
                    for (Message message : messages) {
                        // Process the message
                        AWS.debugMsg("Received message: %s", message.body());

                        // Assuming the message contains the summary file URL in JSON format
                        summaryFileUrl = message.body();
                        AWS.debugMsg("Summary file is available at: %s", summaryFileUrl);

                        // Mark the summary as received and exit loop
                        summaryReceived = true;

                        // Delete the processed message from the queue
                        aws.deleteMessageFromQueue(RESULT_QUEUE_URL, message.receiptHandle());
                        break; // Exit the loop after processing a valid message
                    }
                }
            }
        } catch (Exception e) {
            AWS.errorMsg("Error waiting for summary file: %s", e.getMessage());
        }
        long finishTime = System.currentTimeMillis();
        AWS.debugMsg("Waiting time: %d seconds", (finishTime - startTime)/1000);
        return summaryFileUrl;
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
                String result = parts[2].replace(" ", "");

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
            AWS.errorMsg("Error reading summary file: %s", e.getMessage());
        }

        htmlContent.append("</table>\n</body>\n</html>");

        try (FileWriter writer = new FileWriter(outputHtmlPath)) {
            writer.write(htmlContent.toString());
            AWS.debugMsg("HTML file created: %s", outputHtmlPath);
        } catch (Exception e) {
            AWS.errorMsg("Error writing HTML file: %s", e.getMessage());
        }
    }

    public static void cleanup(boolean terminator){
        // Delete all the queues
//        if (terminator) {
//            for (String queueUrl : QueueUrls) {
//                aws.deleteQueue(queueUrl);
//                QueueUrls.remove(queueUrl);
//            }
//        }
        // Delete all the buckets - files inside the bucket
//        aws.deleteBucket(bucketName);
    }

    public static String generateRandomID(String prefix) {
        String uniqueId = UUID.randomUUID().toString().replace("-", "");
        String id;

        if (prefix == null || prefix.isEmpty()) {
            id = uniqueId;
        } else {
            id = prefix + "-" + uniqueId;
        }

        // Ensure the ID length does not exceed 63 characters
        if (id.length() > 63) {
            id = id.substring(0, 63);
        }

        return id.toLowerCase();
    }
}
