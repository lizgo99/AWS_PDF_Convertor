package LocalApp;

import AWS.AWS;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.HashSet;

public class LocalApp {
    private static final AWS aws = AWS.getInstance();
    private static final HashSet<String> queueUrls = new HashSet<>();
    private static boolean shouldTerminate;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        if (args.length < 3) {
            AWS.errorMsg("Usage: java -jar LocalApp.jar inputFileName outputFileName n [terminate]");
            System.exit(1);
        }

        String inputFileName = args[0];
        String outputFileName = args[1];
        int numOfPdfsPerWorker = Integer.parseInt(args[2]);
        shouldTerminate = args.length == 4 && args[3].equals("terminate");

        try {
            AWS.debugMsg("LocalApp: Starting request processing at " + new java.util.Date());
            processRequest(inputFileName, outputFileName, numOfPdfsPerWorker, shouldTerminate);
            long endTime = System.currentTimeMillis();
            AWS.debugMsg("Total execution time: " + formatDuration(endTime - startTime));
        } catch (Exception e) {
            AWS.errorMsg("Error processing request: " + e.getMessage());
            cleanup();
            System.exit(1);
        }
    }

    private static void processRequest(String inputFileName, String outputFileName, int numOfPdfsPerWorker,
            boolean shouldTerminate) throws Exception {
        long operationStartTime = System.currentTimeMillis();
        // Create necessary queues
        // check if the queue already exists
        String localAppToManagerQueueUrl = aws.createSqsQueue("LocalAppToManager");
        String managerToLocalAppQueueUrl = aws.createSqsQueue("ManagerToLocalApp");
        queueUrls.add(localAppToManagerQueueUrl);
        queueUrls.add(managerToLocalAppQueueUrl);

        // Start manager if not active
        aws.startManagerIfNotActive();

        // Upload input file to S3
        File inputFile = new File(inputFileName);
        if (!inputFile.exists()) {
            throw new FileNotFoundException("Input file not found: " + inputFileName);
        }
        String s3InputFileUrl = aws.uploadFileToS3("inputs/" + inputFileName, inputFile);

        // Send task to manager
        aws.sendMessageToQueue(localAppToManagerQueueUrl, s3InputFileUrl + "\t" + String.valueOf(numOfPdfsPerWorker));

        // Wait for response
        AWS.debugMsg("LocalApp: Waiting for manager to process files at " + new java.util.Date());
        long waitStartTime = System.currentTimeMillis();
        String summaryFileUrl = waitForResponse(managerToLocalAppQueueUrl);
        long waitEndTime = System.currentTimeMillis();
        AWS.debugMsg(
                "LocalApp: Received response from manager. Wait time: " + formatDuration(waitEndTime - waitStartTime));
        if (summaryFileUrl != null) {
            createHtmlOutput(summaryFileUrl, outputFileName);
        }

        // Send terminate message if requested
        if (shouldTerminate) {
            aws.sendMessageToQueue(localAppToManagerQueueUrl, "terminate");
        }

        cleanup();
    }

    /**
     * Waits for the Manager to complete processing all PDF files and return a
     * summary
     * Uses a polling mechanism with a timeout to prevent indefinite waiting
     *
     * @param queueUrl The SQS queue URL where the Manager will send the response
     * @return The S3 URL of the summary file containing all PDF processing results
     * @throws RuntimeException if no response is received within the timeout period
     */
    private static String waitForResponse(String queueUrl) {
        long startTime = System.currentTimeMillis();
        long timeout = 15 * 60 * 1000;

        while (System.currentTimeMillis() - startTime < timeout) {
            Message message = aws.getMessageFromQueue(queueUrl);

            if (message != null) {
                String summaryFileUrl = message.body();
                AWS.debugMsg("Received summary file URL: " + summaryFileUrl);
                aws.deleteMessageFromQueue(queueUrl, message.receiptHandle());
                return summaryFileUrl;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }

        throw new RuntimeException("Timeout waiting for response after " + (timeout / 1000) + " seconds");
    }

    private static void createHtmlOutput(String summaryFileUrl, String outputFileName) {
    }

    private static void cleanup() {
        
    }

    private static String formatDuration(long milliseconds) {
        long seconds = milliseconds / 1000;
        long minutes = seconds / 60;
        seconds = seconds % 60;
        return String.format("%d minutes, %d seconds", minutes, seconds);
    }
}
