package Manager;

import AWS.AWS;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Manager {

    private static final AWS aws = AWS.getInstance();
    private static final int MAX_WORKERS = 8;
    private static String localAppToManagerQueueUrl;
    private static String managerToWorkersQueueUrl;
    private static String workersToManagerQueueUrl;
    private static String managerToLocalAppQueueUrl;
    private static int numOfPdfsPerWorker = -1;

    private static final ConcurrentHashMap<String, TaskTracker> TasksMap = new ConcurrentHashMap<>();
    private static final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private static final ExecutorService messageProcessorService = Executors.newFixedThreadPool(3);
    private static final ExecutorService workerManagerService = Executors.newFixedThreadPool(1);
    private static final Semaphore workerSemaphore = new Semaphore(MAX_WORKERS);

    public static void main(String[] args) {
        localAppToManagerQueueUrl = aws.connectToQueueByName("LocalAppToManager");
        managerToLocalAppQueueUrl = aws.connectToQueueByName("ManagerToLocalApp");
        managerToWorkersQueueUrl = aws.createSqsQueue("ManagerToWorkers");
        workersToManagerQueueUrl = aws.createSqsQueue("WorkersToManager");

        startProcessing();
    }

    private static void startProcessing() {
        AWS.debugMsg("Manager: Starting processing threads");

        // Start message processor threads
        messageProcessorService.submit(() -> {
            AWS.debugMsg("Manager: Starting LocalApp message listener thread");
            while (!shouldTerminate.get()) {
                receiveAndParseMsgFromLocalApp();
            }
            AWS.debugMsg("Manager: LocalApp message listener thread terminated");
        });

        // Start worker manager in a separate executor
        workerManagerService.submit(() -> {
            AWS.debugMsg("Manager: Starting worker manager thread");
            while (!shouldTerminate.get()) {
                manageWorkers();
            }
            AWS.debugMsg("Manager: Worker manager thread terminated");
        });

        messageProcessorService.submit(() -> {
            AWS.debugMsg("Manager: Starting worker response processor thread");
            while (!shouldTerminate.get()) {
                processWorkerResponses();
            }
            AWS.debugMsg("Manager: Worker response processor thread terminated");
        });

        AWS.debugMsg("Manager: All processing threads started, waiting for termination");

        // Wait for termination signal
        while (!shouldTerminate.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Shutdown processing
        AWS.debugMsg("Manager: Initiating shutdown sequence");
        messageProcessorService.shutdown();
        workerManagerService.shutdown();

        try {
            // Wait for all tasks to complete
            if (!messageProcessorService.awaitTermination(30, TimeUnit.SECONDS)) {
                messageProcessorService.shutdownNow();
            }
            if (!workerManagerService.awaitTermination(30, TimeUnit.SECONDS)) {
                workerManagerService.shutdownNow();
            }
            AWS.debugMsg("Manager: All threads terminated successfully");
        } catch (InterruptedException e) {
            AWS.debugMsg("Manager: Thread termination interrupted: " + e.getMessage());
            messageProcessorService.shutdownNow();
            workerManagerService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        cleanup();
    }

    private static void receiveAndParseMsgFromLocalApp() {
        try {
            List<Message> messages = aws.pollMessages(localAppToManagerQueueUrl);
            if (messages != null && !messages.isEmpty()) {
                AWS.debugMsg("Manager: Received " + messages.size() + " messages from LocalApp");
                for (Message message : messages) {
                    AWS.debugMsg("Manager: Submitting message for processing: " + message.body());
                    try {
                        processLocalAppMessage(message);
                    } catch (Exception e) {
                        AWS.errorMsg("Manager: Error submitting message for processing: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error in receiveAndParseMsgFromLocalApp: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processLocalAppMessage(Message message) {
        if (message == null) {
            AWS.errorMsg("Manager: Received null message");
            return;
        }

        AWS.debugMsg("Manager: Starting to process LocalApp message: " + message.body());
        try {
            String[] messageBody = message.body().split("\t");
            AWS.debugMsg("Manager: Message parts length: " + messageBody.length);

            if (messageBody.length == 2) {
                if (messageBody[0].equals("terminate")) {
                    AWS.debugMsg("Manager: Received terminate command");
                    shouldTerminate.set(true);
                    aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
                    cleanup();
                    return;
                }
                aws.changeVisibilityTimeout(localAppToManagerQueueUrl, message.receiptHandle(), 120);
                String inputS3FileUrl = messageBody[0];
                String fileName = inputS3FileUrl.substring(inputS3FileUrl.lastIndexOf('/') + 1);
                String keyPath = "inputs/" + fileName;
                numOfPdfsPerWorker = Integer.parseInt(messageBody[1]);
                AWS.debugMsg("Manager: Processing input file: " + fileName + " with " + numOfPdfsPerWorker
                        + " PDFs per worker");

                int taskCount = 0;
                try (BufferedReader reader = aws.downloadFileFromS3(keyPath)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        AWS.debugMsg("Manager: Sending task to workers queue: " + line);
                        aws.sendMessageToQueue(managerToWorkersQueueUrl, line);
                        taskCount++;
                    }
                    AWS.debugMsg("Manager: Created " + taskCount + " tasks for file: " + fileName);
                    TasksMap.put(inputS3FileUrl, new TaskTracker(inputS3FileUrl, taskCount));
                }
                aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
                AWS.debugMsg("Manager: Successfully processed and deleted message from queue");
            } else {
                AWS.errorMsg("Manager: Invalid message format received: " + message.body() + ". Expected 2 parts, got "
                        + messageBody.length);
                aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
            }
        } catch (IOException e) {
            AWS.errorMsg("Manager: Error processing message: " + e.getMessage());
            e.printStackTrace();
            aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
        } catch (Exception e) {
            AWS.errorMsg("Manager: Unexpected error processing message: " + e.getMessage());
            e.printStackTrace();
            aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
        }
    }

    private static void processWorkerResponses() {
        List<Message> messages = aws.pollMessages(workersToManagerQueueUrl);
        if (messages != null && !messages.isEmpty()) {
            AWS.debugMsg("Manager: Received " + messages.size() + " responses from workers");
            for (Message message : messages) {
                messageProcessorService.submit(() -> processWorkerMessage(message));
            }
        }
    }

    private static void processWorkerMessage(Message message) {
        AWS.debugMsg("Manager: Processing worker message: " + message.body());
        try {
            String[] parts = message.body().split("\t");
            if (parts.length == 3) {
                String operation = parts[0];
                String pdfUrl = parts[1];
                String outputUrlOrErrorMsg = parts[2];

                AWS.debugMsg("Manager: Worker completed operation: " + operation + " for PDF: " + pdfUrl);

                for (TaskTracker taskTracker : TasksMap.values()) {
                    if (taskTracker.addResult(pdfUrl, operation + ": " + pdfUrl + " " + outputUrlOrErrorMsg)) {
                        AWS.debugMsg("Manager: Added result to task tracker for: " + pdfUrl);
                        if (taskTracker.isAllCompleted()) {
                            AWS.debugMsg(
                                    "Manager: All tasks completed for input file: " + taskTracker.getInputFileUrl());
                            messageProcessorService.submit(() -> createAndSendSummaryFile(taskTracker));
                        }
                        break;
                    }
                }
            }
            aws.deleteMessageFromQueue(workersToManagerQueueUrl, message.receiptHandle());
            AWS.debugMsg("Manager: Successfully processed and deleted worker message from queue");
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error processing worker response: " + e.getMessage());
        }
    }

    private static void createAndSendSummaryFile(TaskTracker taskTracker) {
        AWS.debugMsg("Manager: Creating summary file for: " + taskTracker.getInputFileUrl());
        try {
            File summaryFile = File.createTempFile("summary", ".txt");
            String summaryKey = "summaries/" + summaryFile.getName();

            try (PrintWriter writer = new PrintWriter(summaryFile)) {
                for (String result : taskTracker.getResults().values()) {
                    writer.println(result);
                }
            }

            String summaryFileUrl = aws.uploadFileToS3(summaryKey, summaryFile);
            AWS.debugMsg("Manager: Uploaded summary file to S3: " + summaryFileUrl);
//            aws.makeFolderPublic("outputs");
            aws.sendMessageToQueue(managerToLocalAppQueueUrl, summaryFileUrl);
            TasksMap.remove(taskTracker.getInputFileUrl());
            Files.delete(summaryFile.toPath());
            AWS.debugMsg("Manager: Summary file processed and cleaned up successfully");
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error creating/uploading summary file: " + e.getMessage());
        }
    }

    private static void manageWorkers() {
        try {
            int totalPendingTasks = aws.getQueueMessageCount(managerToWorkersQueueUrl);
            if (totalPendingTasks > 0 && numOfPdfsPerWorker > 0) {
                int requiredWorkers = Math.min((totalPendingTasks + numOfPdfsPerWorker - 1) / numOfPdfsPerWorker,
                        MAX_WORKERS);
                int currentWorkers = MAX_WORKERS - workerSemaphore.availablePermits();
                int workersToStart = Math.max(0, requiredWorkers - currentWorkers);

                AWS.debugMsg(String.format(
                        "Manager: Worker status - Pending tasks: %d, Current workers: %d, Required workers: %d, Workers to start: %d",
                        totalPendingTasks, currentWorkers, requiredWorkers, workersToStart));

                if (workersToStart > 0) {
                    AWS.debugMsg("Manager: Starting " + workersToStart + " new worker instance(s)");
                    // Start workers directly instead of using executor service
                    if (workerSemaphore.tryAcquire(workersToStart)) {
                        try {
                            aws.startWorkerInstances(workersToStart);
                            AWS.debugMsg("Manager: Successfully started " + workersToStart + " worker instances");
                        } catch (Exception e) {
                            AWS.errorMsg("Manager: Failed to start worker instances: " + e.getMessage());
                            workerSemaphore.release(workersToStart);
                        }
                    }
                }
            }
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            AWS.errorMsg("Manager: Worker management thread interrupted: " + e.getMessage());
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error in worker management: " + e.getMessage());
        }
    }

    private static void terminateAllWorkers() {
        AWS.debugMsg("Manager: Terminating all worker instances");
        aws.terminateAllWorkerInstances();

        // Delete worker queues
        try {
            aws.deleteQueue(managerToWorkersQueueUrl);
            aws.deleteQueue(workersToManagerQueueUrl);
            AWS.debugMsg("Manager: Worker queues deleted successfully");
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error deleting worker queues: " + e.getMessage());
        }
    }

    private static void cleanup() {
        AWS.debugMsg("Manager: Starting cleanup process");
        terminateAllWorkers();

        // Wait for any remaining tasks to complete
        try {
            Thread.sleep(5000); // Give some time for final tasks to complete
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Delete manager queues
        try {
            aws.deleteQueue(localAppToManagerQueueUrl);
            aws.deleteQueue(managerToLocalAppQueueUrl);
            AWS.debugMsg("Manager: Manager queues deleted successfully");
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error deleting manager queues: " + e.getMessage());
        }

        aws.cleanup();
        System.exit(0);
    }
}
