import jdk.internal.net.http.common.Pair;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class Manager {

    private static final AWS aws = AWS.getInstance();
    private static final int MAX_WORKERS = 8;
    private static String localAppToManagerQueueUrl;
    private static String managerToWorkersQueueUrl;
    private static String workersToManagerQueueUrl;
    private static String managerToLocalAppQueueUrl;
    private static int numOfPdfsPerWorker = -1;
    // TODO: save bucketName if necessary.
    // private static String bucketName;

    private static final ConcurrentHashMap<String, TaskTracker> TasksMap = new ConcurrentHashMap<>();
    private static final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private static final ExecutorService localAppMessageProcessorService = Executors.newFixedThreadPool(3);
    private static final ExecutorService workersMessageProcessorService = Executors.newFixedThreadPool(3);
//    private static final ExecutorService messageProcessorService = Executors.newFixedThreadPool(3);
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
        localAppMessageProcessorService.submit(() -> {
            AWS.debugMsg("Manager: Starting LocalApp message listener thread");
            // Stop listening to Local Apps when receiving a termination signal
            while (!shouldTerminate.get() || !TasksMap.isEmpty()) {
                receiveAndParseMsgFromLocalApp();
            }
            AWS.debugMsg("Manager: LocalApp message listener thread terminated");
        });

        // Start worker manager in a separate executor
        workerManagerService.submit(() -> {
            AWS.debugMsg("Manager: Starting worker manager thread");
            while (!shouldTerminate.get() || !TasksMap.isEmpty()) {
                manageWorkers();
            }
            AWS.debugMsg("Manager: Worker manager thread terminated");
        });

        workersMessageProcessorService.submit(() -> {
            AWS.debugMsg("Manager: Starting worker response processor thread");
            while (!shouldTerminate.get() || !TasksMap.isEmpty()) {
                processWorkerResponses();
            }
            AWS.debugMsg("Manager: Worker response processor thread terminated");
        });

        AWS.debugMsg("Manager: All processing threads started, waiting for termination");

        // Wait for termination signal and completion of all tasks
        while (!shouldTerminate.get() || !TasksMap.isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Shutdown processing
        AWS.debugMsg("Manager: Initiating shutdown sequence");
        shutdownAndCleanup();
    }

    private static void receiveAndParseMsgFromLocalApp() {
        try {
            List<Message> messages = aws.pollMessages(localAppToManagerQueueUrl);
            if (messages != null && !messages.isEmpty()) {
                AWS.debugMsg("Manager: Received %d messages from LocalApp", messages.size());
                for (Message message : messages) {
                    AWS.debugMsg("Manager: Submitting message for processing: %s", message.body());
                    try {
                        processLocalAppMessage(message);
                    } catch (Exception e) {
                        AWS.errorMsg("Manager: Error submitting message for processing: %s", e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error in receiveAndParseMsgFromLocalApp: %s", e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processLocalAppMessage(Message message) {
        if (message == null) {
            AWS.errorMsg("Manager: Received null message");
            return;
        }

        AWS.debugMsg("Manager: Starting to process LocalApp message: %s", message.body());
        try {
            String[] messageBody = message.body().split("\t");
            AWS.debugMsg("Manager: Message parts length: %d", messageBody.length);

            if (messageBody.length == 1) {
                if (messageBody[0].equals("terminate")) {
                    AWS.debugMsg("Manager: Received terminate command");
                    shouldTerminate.set(true);
                    aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
                    // cleanup();
                } else {

                    TasksMap.remove(messageBody[0]);
                }
                return;
            }

            if (messageBody.length == 2) {
                aws.changeVisibilityTimeout(localAppToManagerQueueUrl, message.receiptHandle(), 120);
                String inputS3FileUrl = messageBody[0];
                String fileName = inputS3FileUrl.substring(inputS3FileUrl.lastIndexOf('/') + 1);
                String bucketName = inputS3FileUrl.substring(5, inputS3FileUrl.indexOf('/', 5));
                String keyPath = "inputs/" + fileName;
                numOfPdfsPerWorker = Integer.parseInt(messageBody[1]);
                AWS.debugMsg("Manager: Processing input file: %s with %d PDFs per worker", fileName,
                        numOfPdfsPerWorker);
                AWS.debugMsg("Manager: Using bucket: %s, keyPath: %s", bucketName, keyPath);
                TaskTracker taskTracker = new TaskTracker(inputS3FileUrl, 0);
                TasksMap.putIfAbsent(bucketName, taskTracker);
                AWS.debugMsg("Manager: Created task tracker for ID: %s:", bucketName);

                 int taskCount = 0;
//                AtomicInteger taskCount = new AtomicInteger(0);
                try (BufferedReader reader = aws.downloadFileFromS3(bucketName, keyPath)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        AWS.debugMsg("Manager: Sending task to workers queue: %s", line);
                        aws.sendMessageToQueue(managerToWorkersQueueUrl, line + "\t" + bucketName);
                         taskCount++;
//                        taskCount.incrementAndGet();
                    }
                    AWS.debugMsg("Manager: Created %d tasks for file: %s", taskCount, fileName);
                }
                int newTotal = taskTracker.changeTotalTasks(taskCount);
                AWS.debugMsg("Manager: Changed total task number to %d in tracker %s", newTotal, bucketName);
                aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
                AWS.debugMsg("Manager: Successfully processed and deleted message from queue");
            } else {
                AWS.errorMsg("Manager: Invalid message format received: %s. Expected 2 parts, got %d", message.body(),
                        messageBody.length);
                aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
            }
        } catch (IOException e) {
            AWS.errorMsg("Manager: Error processing message: %s", e.getMessage());
            e.printStackTrace();
            aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
        } catch (Exception e) {
            AWS.errorMsg("Manager: Unexpected error processing message: %s", e.getMessage());
            e.printStackTrace();
            aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
        }
    }

    private static void processWorkerResponses() {
        List<Message> messages = aws.pollMessages(workersToManagerQueueUrl);
        if (messages != null && !messages.isEmpty()) {
            AWS.debugMsg("Manager: Received %d responses from workers", messages.size());
            for (Message message : messages) {
                workersMessageProcessorService.submit(() -> processWorkerMessage(message));
            }
        }
    }

    private static void processWorkerMessage(Message message) {
        AWS.debugMsg("Manager: Processing worker message: %s", message.body());
        try {
            String[] parts = message.body().split("\t");
            if (parts.length == 4) {
                String operation = parts[0];
                String pdfUrl = parts[1];
                String outputUrlOrErrorMsg = parts[2];
                String bucket = parts[3];

                AWS.debugMsg("Manager: Worker completed operation: %s for PDF: %s", operation, pdfUrl);
                TaskTracker taskTracker = TasksMap.get(bucket);
                AWS.debugMsg("is taskTracker null: %b", taskTracker == null);
                if (taskTracker == null) {
                    AWS.errorMsg("No task tracker for ID: %s", bucket);
                } else {
                    taskTracker.addResultToList(operation + ": " + pdfUrl + " " + outputUrlOrErrorMsg);
                    AWS.debugMsg("Manager: Added result to task tracker %s for: %s", bucket, pdfUrl);
                    if (taskTracker.isAllCompleted()) {
                        AWS.debugMsg("Manager: All tasks completed for input file: %s", taskTracker.getInputFileUrl());
                        workersMessageProcessorService.submit(() -> createAndSendSummaryFile(taskTracker, bucket));
                    }
                }
            }
            aws.deleteMessageFromQueue(workersToManagerQueueUrl, message.receiptHandle());
            AWS.debugMsg("Manager: Successfully processed and deleted worker message from queue");
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error processing worker response: %s", e.getMessage());
        }
    }

    private static void createAndSendSummaryFile(TaskTracker taskTracker, String bucketName) {
        AWS.debugMsg("Manager: Creating summary file for: %s", taskTracker.getInputFileUrl());
        try {
            File summaryFile = File.createTempFile("summary", ".txt");
            String summaryKey = "summaries/" + summaryFile.getName();

            try (PrintWriter writer = new PrintWriter(summaryFile)) {
                ConcurrentLinkedQueue<String> resultsList = taskTracker.getResultsList();
                for (String result : resultsList) {
                    writer.println(result);
                }
//                ConcurrentHashMap<String, LongAdder> results = taskTracker.getResults();
//                for (String result : results.keySet()) {
//                    for (int i = 0; i < results.get(result).intValue(); i++){
//                        writer.println(result);
//                    }
//                }
            }

            String summaryFileUrl = aws.uploadFileToS3(bucketName, summaryKey, summaryFile);
            AWS.debugMsg("Manager: Uploaded summary file to S3: %s", summaryFileUrl);
            aws.sendMessageToQueue(managerToLocalAppQueueUrl, summaryFileUrl);
            //
            AWS.debugMsg("Manager: Deleted task Tracker for ID: %s", bucketName);
            Files.delete(summaryFile.toPath());
            AWS.debugMsg("Manager: Summary file processed and cleaned up successfully");
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error creating/uploading summary file: %s", e.getMessage());
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

                AWS.debugMsg(
                        "Manager: Worker status - Pending tasks: %d, Current workers: %d, Required workers: %d, Workers to start: %d",
                        totalPendingTasks, currentWorkers, requiredWorkers, workersToStart);

                if (workersToStart > 0) {
                    AWS.debugMsg("Manager: Starting %d new worker instance(s)", workersToStart);
                    if (workerSemaphore.tryAcquire(workersToStart)) {
                        try {
                            aws.startWorkerInstances(workersToStart);
                            AWS.debugMsg("Manager: Successfully started %d worker instances", workersToStart);
                        } catch (Exception e) {
                            AWS.errorMsg("Manager: Failed to start worker instances: %s", e.getMessage());
                            workerSemaphore.release(workersToStart);
                        }
                    }
                }
            }
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            AWS.errorMsg("Manager: Worker management thread interrupted: %s", e.getMessage());
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error in worker management: %s", e.getMessage());
        }
    }

    public static void shutdownAndCleanup() {
        AWS.debugMsg("Manager: Starting shutdown sequence");

        // First, stop accepting new tasks
        localAppMessageProcessorService.shutdown();
        workersMessageProcessorService.shutdown();
        workerManagerService.shutdown();

        try {
            if (!localAppMessageProcessorService.awaitTermination(30, TimeUnit.SECONDS)) {
                localAppMessageProcessorService.shutdownNow();
            }
            if (!workersMessageProcessorService.awaitTermination(30, TimeUnit.SECONDS)) {
                workersMessageProcessorService.shutdownNow();
            }
            if (!workerManagerService.awaitTermination(30, TimeUnit.SECONDS)) {
                workerManagerService.shutdownNow();
            }
            AWS.debugMsg("Manager: All threads terminated successfully");
        } catch (InterruptedException e) {
            AWS.debugMsg("Manager: Thread termination interrupted: %s", e.getMessage());
            localAppMessageProcessorService.shutdownNow();
            workersMessageProcessorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        AWS.debugMsg("Manager: Terminating all worker instances");
        aws.terminateAllRunningWorkerInstances();

        // Wait for workers to finish terminating and for any pending S3 operations
        try {
            Thread.sleep(30000); // Wait 30 seconds for workers to terminate and S3 operations to complete
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Delete queues only after ensuring all operations are complete
        deleteQueues();

        // Finally cleanup AWS clients
        aws.cleanup();

        AWS.debugMsg("Manager: Cleanup completed successfully");
    }

    private static void deleteQueues() {
        waitForQueuesToEmpty();
        try {
            AWS.debugMsg("is managerToWorkersQueueUrl null: %b", managerToWorkersQueueUrl == null);
            if (managerToWorkersQueueUrl != null) {
                aws.deleteQueue(managerToWorkersQueueUrl);
            }
            AWS.debugMsg("is workersToManagerQueueUrl null: %b", workersToManagerQueueUrl == null);
            if (workersToManagerQueueUrl != null) {
                aws.deleteQueue(workersToManagerQueueUrl);
            }
            AWS.debugMsg("is localAppToManagerQueueUrl null: %b", localAppToManagerQueueUrl == null);
            if (localAppToManagerQueueUrl != null) {
                aws.deleteQueue(localAppToManagerQueueUrl);
            }
            AWS.debugMsg("is managerToLocalAppQueueUrl null: %b", managerToLocalAppQueueUrl == null);
            if (managerToLocalAppQueueUrl != null) {
                aws.deleteQueue(managerToLocalAppQueueUrl);
            }
        } catch (Exception e) {
            AWS.errorMsg("Manager: Error during queue cleanup: %s", e.getMessage());
        }
    }

    private static void waitForQueuesToEmpty() {
        AWS.debugMsg("Waiting for queues to be empty before deletion...");

        boolean queuesEmpty = false;
        int maxWaitSeconds = 60; // Maximum time to wait
        int waitedSeconds = 0;

        while (!queuesEmpty && waitedSeconds < maxWaitSeconds) {
            int mtw = aws.getQueueMessageCount(managerToWorkersQueueUrl);
            int wtm = aws.getQueueMessageCount(workersToManagerQueueUrl);
            int ltm = aws.getQueueMessageCount(localAppToManagerQueueUrl);
            int mtl = aws.getQueueMessageCount(managerToLocalAppQueueUrl);

            AWS.debugMsg("Queue message counts - MTW: %d, WTM: %d, LTM: %d, MTL: %d",
                    mtw, wtm, ltm, mtl);

            if (mtw == 0 && wtm == 0 && ltm == 0 && mtl == 0) {
                queuesEmpty = true;
                AWS.debugMsg("All queues are empty");
            } else {
                try {
                    Thread.sleep(1000);
                    waitedSeconds++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        if (!queuesEmpty) {
            AWS.debugMsg("Warning: Timeout waiting for queues to empty");
        }
    }

    // private static void cleanup() {
    // AWS.debugMsg("Manager: Starting cleanup process");

    // // First, terminate all worker instances
    // terminateAllWorkers();

    // // Wait for any remaining tasks to complete
    // try {
    // Thread.sleep(10000); // Give more time for final tasks to complete
    // } catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // }

    // // Delete all queues used in the system
    // AWS.debugMsg("Manager: Deleting all queues");
    // try {
    // // Delete all queues
    // aws.deleteQueue(managerToWorkersQueueUrl);
    // aws.deleteQueue(workersToManagerQueueUrl);
    // aws.deleteQueue(localAppToManagerQueueUrl);
    // aws.deleteQueue(managerToLocalAppQueueUrl);

    // // Wait for queue deletions to complete
    // Thread.sleep(5000);

    // AWS.debugMsg("Manager: All queues deleted successfully");
    // } catch (Exception e) {
    // AWS.errorMsg("Manager: Error during queue cleanup: %s", e.getMessage());
    // }

    // // // Finally cleanup AWS resources
    // // aws.cleanup();

    // AWS.debugMsg("Manager: Cleanup completed, terminating manager instance");
    // System.exit(0);
    // }

}
