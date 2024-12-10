import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LocalApp {
    final static AWS aws = AWS.getInstance();
    private static final String ID = generateRandomID("");
    private static final String REJECT = "Rejected";
    private static String ManagerToLocalAppQueueUrl = null;
    public static String LocalAppToManager = "LocalAppToManager";
    public static String ManagerToLocalApp = "ManagerToLocalApp";

    public static void main(String[] args) throws Exception {
        // Read from terminal >java -jar yourjar.jar inputFileName outputFileName n [terminate]/[purge]
        if (args.length < 3) {
            AWS.errorMsg("Usage: java -jar LocalApp/target/LocalApp.jar inputFileName outputFileName n [terminate]");
            System.exit(1);
        }

        String inputFileName = args[0];
        AWS.debugMsg("inputFileName: %s", inputFileName);
        String outputFileName = args[1];
        AWS.debugMsg("outputFileName: %s", outputFileName);
        int pdfsPerWorker = Integer.parseInt(args[2]);
        boolean terminate = args.length > 3 && args[3].equals("terminate");
        boolean purge = args.length > 3 && args[3].equals("purge");
        File inputFile = new File(inputFileName);

        // Upload jars if necessary
        aws.addJarsIfNotExists();

        // Create manager if one doesn't exist
        aws.startManagerIfNotActive();

        // Create a bucket and upload the file
        aws.createBucketIfNotExists(ID);
        String fileLocation = aws.uploadFileToS3(ID, "inputs/" + inputFile.getName(), inputFile);

        // Create a new SQS queue
        ManagerToLocalAppQueueUrl = aws.createSqsQueue(LocalAppToManager);

        // Upload file location to the SQS
        AWS.debugMsg("fileLocation: %s", fileLocation);
        aws.sendMessageToQueue(LocalAppToManager, fileLocation + "\t" + pdfsPerWorker);

        ManagerToLocalAppQueueUrl = aws.createSqsQueue(ManagerToLocalApp);

        // Handle termination
        if (terminate || purge) {
            new Thread(() -> {
                try {
                    Thread.sleep(10000); // Wait 10 seconds to prevent racing condition
                    AWS.debugMsg("Sending delayed termination signal to Manager");
                    aws.sendMessageToQueue(LocalAppToManager, "terminate");
                } catch (InterruptedException e) {
                    AWS.errorMsg("Termination thread interrupted: %s", e.getMessage());
                    Thread.currentThread().interrupt();
                }
            }).start();
        }

        String summeryURL = waitForSummaryFile();

        if (summeryURL != null && !summeryURL.equals(REJECT)) {
            if (!outputFileName.endsWith(".html")) {outputFileName = outputFileName + ".html";}
            createHTMLFile(aws.downloadFileFromS3(ID, summeryURL), (new File(outputFileName)).getPath());
            aws.makeFolderPublic(ID, "outputs");
        }


        // Clean up resources
        if (purge) {aws.deleteAllBuckets();}

        aws.close();
        AWS.debugMsg("Exiting");
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
                            if (message != null && message.body().contains(ID)) {
                                if (message.body().contains("\t")) {
                                    AWS.errorMsg("Manger is terminating");
                                    aws.deleteMessageFromQueue(ManagerToLocalAppQueueUrl, message.receiptHandle());
                                    return REJECT;
                                }

                                // Process the message
                                AWS.debugMsg("Received message: %s", message.body());
                                // Assuming the message contains the summary file URL
                                summaryFileUrl = message.body();
                                AWS.debugMsg("Summary file is available at: %s", summaryFileUrl);

                                // Mark the summary as received and exit loop
                                summaryReceived = true;

                                // Delete the processed message from the queue
                                aws.deleteMessageFromQueue(ManagerToLocalAppQueueUrl, message.receiptHandle());
                            }
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

    public static void createHTMLFile(BufferedReader summaryFileReader, String outputHtmlPath) {
        StringBuilder htmlContent = new StringBuilder();
        htmlContent.append("<!DOCTYPE html>\n<html>\n<head>\n<title>PDF Processing Summary</title>\n</head>\n<body>\n");
        htmlContent.append("<h1>PDF Processing Summary</h1>\n");
        htmlContent.append("<table border=\"1\">\n<tr><th>Operation</th><th>Input File</th><th>Result</th></tr>\n");

        try {
            String line;
            while ((line = summaryFileReader.readLine()) != null) {
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
