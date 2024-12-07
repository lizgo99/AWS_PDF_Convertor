import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.util.*;

public class LocalApp {
    final static AWS aws = AWS.getInstance();
    // public static String inputFileName = "input-sample-2.txt";
    private static final String ID = generateRandomID("");
    public static HashSet<String> QueueUrls = new HashSet<>();
    private static String ManagerToLocalAppQueueUrl = null;
    public static String localAppToManager = "LocalAppToManager";
    public static String ManagerToLocalApp = "ManagerToLocalApp";

    public static void main(String[] args) throws Exception {
        // read from terminal >java -jar yourjar.jar inputFileName outputFileName n
        // [terminate]
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
        String fileLocation = aws.uploadFileToS3(ID, "inputs/" + inputFile.getName(), inputFile);

        // Create a new SQS queue
        String queueName = "LocalAppToManager";
        String queueUrl = aws.createSqsQueue(queueName);
        // QueueUrls.add(queueUrl);

        // Upload file location to the SQS
        AWS.debugMsg("fileLocation: %s", fileLocation);
        aws.sendMessageToQueue(queueUrl, fileLocation + "\t" + pdfsPerWorker);

        ManagerToLocalAppQueueUrl = aws.createSqsQueue(ManagerToLocalApp);
        // QueueUrls.add(ManagerToLocalAppQueueUrl);

        String summeryURL = waitForSummaryFile();

        if (summeryURL != null) {
            File summeryFile = new File("summery.txt");
            try (BufferedReader output = aws.downloadFileFromS3(ID, summeryURL);
                    FileWriter writer = new FileWriter(summeryFile)) {
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

        aws.makeFolderPublic(ID, "outputs");

        // Clean up resources
        // cleanup();

        // Handle termination
        if (terminate) {
            AWS.debugMsg("LocalApp: Sending termination message to Manager");
            aws.sendMessageToQueue(localAppToManager, "terminate");
            // Wait briefly to ensure the termination message is sent
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        AWS.debugMsg("LocalApp: Exiting");
    }

    public static String waitForSummaryFile() {

        String summaryFileUrl = null;
        long startTime = System.currentTimeMillis();
        try {
            AWS.debugMsg("Waiting for summary file...");

            boolean summaryReceived = false;

            while (!summaryReceived) {
                // Poll the ResultQueue for a message
                List<Message> messages = aws.pollMessages(ManagerToLocalAppQueueUrl);
                if (messages != null) {
                    for (Message message : messages) {
                        // Process the message
                        AWS.debugMsg("Received message: %s", message.body());

                        // Assuming the message contains the summary file URL in JSON format
                        summaryFileUrl = message.body();
                        AWS.debugMsg("Summary file is available at: %s", summaryFileUrl);
                        String bucketName = summaryFileUrl.substring(5, summaryFileUrl.indexOf('/', 5));
                        aws.sendMessageToQueue("LocalAppToManager", bucketName);
                        AWS.debugMsg("Sent message to Manager with bucket name: %s", bucketName);

                        // Mark the summary as received and exit loop
                        summaryReceived = true;

                        // Delete the processed message from the queue
                        aws.deleteMessageFromQueue(ManagerToLocalAppQueueUrl, message.receiptHandle());
                        break; // Exit the loop after processing a valid message
                    }
                }
            }
        } catch (Exception e) {
            AWS.errorMsg("Error waiting for summary file: %s", e.getMessage());
        }
        long finishTime = System.currentTimeMillis();
        AWS.debugMsg("Waiting time: %d seconds", (finishTime - startTime) / 1000);
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
                if (parts.length < 3)
                    continue; // Skip malformed lines
                String operation = parts[0];
                String inputUrl = parts[1];
                String resultUrlOrErrMsg = parts[2].replace(" ", "");

                htmlContent.append("<tr>");
                htmlContent.append("<td>").append(operation).append("</td>");
                htmlContent.append("<td><a href=\"").append(inputUrl).append("\">").append(inputUrl)
                        .append("</a></td>");
                if (resultUrlOrErrMsg.startsWith("https://")) {
                    htmlContent.append("<td><a href=\"").append(resultUrlOrErrMsg).append("\">Output File</a></td>");
                } else {
                    htmlContent.append("<td>").append(resultUrlOrErrMsg).append("</td>");
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

    public static void cleanup(boolean terminator) {
        // Delete all the queues
        // if (terminator) {
        // for (String queueUrl : QueueUrls) {
        // aws.deleteQueue(queueUrl);
        // QueueUrls.remove(queueUrl);
        // }
        // }
        // Delete all the buckets - files inside the bucket
        // aws.deleteBucket(bucketName);
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