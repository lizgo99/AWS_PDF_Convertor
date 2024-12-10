import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
// import java.util.concurrent.atomic.AtomicInteger;

import software.amazon.awssdk.services.sqs.model.Message;

public class Manager {

    private static final AWS aws = AWS.getInstance();
    private static final int MAX_WORKERS = 8;
    private static final long MAX_TIME_WAITING = 60000;
    private static String localAppToManagerQueueUrl;
    private static String managerToWorkersQueueUrl;
    private static String workersToManagerQueueUrl;
    private static String managerToLocalAppQueueUrl;
    private static int numOfPdfsPerWorker = -1;
    private static volatile AtomicLong lastWorkerResponse = new AtomicLong(Long.MAX_VALUE);

    private static final ConcurrentHashMap<String, TaskTracker> TasksMap = new ConcurrentHashMap<>();
    private static final AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    private static final ExecutorService localAppMessageProcessorService = Executors.newFixedThreadPool(3);
    private static final ExecutorService workersMessageProcessorService = Executors.newFixedThreadPool(3);
    // private static final ExecutorService messageProcessorService = Executors.newFixedThreadPool(3);
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
        AWS.debugMsg("Starting processing threads");

        // Start message processor threads
        localAppMessageProcessorService.submit(() -> {
            AWS.debugMsg("Starting LocalApp message listener thread");
            // Stop listening to Local Apps when receiving a termination signal
            while (!shouldTerminate.get()  || !TasksMap.isEmpty()) {
                receiveAndParseMsgFromLocalApp();
            }
            AWS.debugMsg("LocalApp message listener thread terminated");
        });

        // Start worker manager in a separate executor
        workerManagerService.submit(() -> {
            AWS.debugMsg("Starting worker manager thread");
            while (!shouldTerminate.get() || !TasksMap.isEmpty()) {
                manageWorkers();
            }
            AWS.debugMsg("Worker manager thread terminated");
        });

        lastWorkerResponse.set(System.currentTimeMillis());
        workersMessageProcessorService.submit(() -> {
            AWS.debugMsg("Starting worker response processor thread");
            while (!shouldTerminate.get() || !TasksMap.isEmpty()) {
                if (processWorkerResponses()){
                    lastWorkerResponse.set(System.currentTimeMillis());
                }
            }
            AWS.debugMsg("Worker response processor thread terminated");
        });

        AWS.debugMsg("All processing threads started, waiting for termination");

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
        AWS.debugMsg("Initiating shutdown sequence");
        shutdownAndCleanup();
    }

    private static void receiveAndParseMsgFromLocalApp() {
        try {
            List<Message> messages = aws.pollMessages(localAppToManagerQueueUrl);
            if (messages != null) {
                for (Message message : messages) {
                    if (!shouldTerminate.get()) {
                        try {
                            AWS.debugMsg("Submitting message for processing: %s", message.body());
                            processLocalAppMessage(message);
                        } catch (Exception e) {
                            AWS.errorMsg("Error submitting message for processing: %s", e.getMessage());
                            e.printStackTrace();
                        }
                    } else {
                        // Rejecting when termination signal received
                        aws.sendMessageToQueue(managerToLocalAppQueueUrl, message.body());
                        aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
                    }
                }
            }
        } catch (Exception e) {
            AWS.errorMsg("Error in receiveAndParseMsgFromLocalApp: %s", e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processLocalAppMessage(Message message) {
        if (message == null) {
            AWS.errorMsg("Received null message");
            return;
        }
        String messageBody = message.body();
        String[] splitMessageBody = message.body().split("\t");
        AWS.debugMsg("Starting to process LocalApp message: %s", messageBody);

        aws.deleteMessageFromQueue(localAppToManagerQueueUrl, message.receiptHandle());
        AWS.debugMsg("Message parts length: %d", splitMessageBody.length);

        try {
            if (splitMessageBody.length == 1) {
                if (splitMessageBody[0].equals("terminate")) {
                    AWS.debugMsg("Received terminate command");
                    shouldTerminate.set(true);
                }
                return;
            }

            if (splitMessageBody.length == 2) {
                String inputS3FileUrl = splitMessageBody[0];
                String fileName = inputS3FileUrl.substring(inputS3FileUrl.lastIndexOf('/') + 1);
                String bucketName = inputS3FileUrl.substring(5, inputS3FileUrl.indexOf('/', 5));
                String keyPath = "inputs/" + fileName;
                numOfPdfsPerWorker = Integer.parseInt(splitMessageBody[1]);
                AWS.debugMsg("Processing input file: %s with %d PDFs per worker", fileName,
                        numOfPdfsPerWorker);
                AWS.debugMsg("Using bucket: %s, keyPath: %s", bucketName, keyPath);
                TaskTracker taskTracker = new TaskTracker(inputS3FileUrl, 0);
                TasksMap.putIfAbsent(bucketName, taskTracker);
                AWS.debugMsg("Created task tracker for ID: %s:", bucketName);

                int taskCount = 0;
                try (BufferedReader reader = aws.downloadFileFromS3(bucketName, keyPath)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        AWS.debugMsg("Sending task to workers queue: %s", line);
                        aws.sendMessageToQueue(managerToWorkersQueueUrl, line + "\t" + bucketName);
                        taskCount++;
                    }
                    AWS.debugMsg("Created %d tasks for file: %s", taskCount, fileName);
                }
                int newTotal = taskTracker.changeTotalTasks(taskCount);
                AWS.debugMsg("Changed total task number to %d in tracker %s", newTotal, bucketName);
                AWS.debugMsg("Successfully processed and deleted message from queue");
            } else {
                AWS.errorMsg("Invalid message format received: %s. Expected 2 parts, got %d", messageBody,
                        splitMessageBody.length);
            }
        } catch (IOException e) {
            AWS.errorMsg("Error processing message: %s", e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            AWS.errorMsg("Unexpected error processing message: %s", e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean processWorkerResponses() {
        List<Message> messages = aws.pollMessages(workersToManagerQueueUrl);
        if (messages != null && !messages.isEmpty()) {
            AWS.debugMsg("Received %d responses from workers", messages.size());
            for (Message message : messages) {
                workersMessageProcessorService.submit(() -> processWorkerMessage(message));
            }
            return true;
        }
        return false;
    }

    private static void processWorkerMessage(Message message) {
        AWS.debugMsg("Processing worker message: %s", message.body());
        try {
            String[] parts = message.body().split("\t");
            aws.deleteMessageFromQueue(workersToManagerQueueUrl, message.receiptHandle());
            if (parts.length == 4) {
                String operation = parts[0];
                String pdfUrl = parts[1];
                String outputUrlOrErrorMsg = parts[2];
                String bucket = parts[3];

                AWS.debugMsg("Worker completed operation: %s for PDF: %s", operation, pdfUrl);
                TaskTracker taskTracker = TasksMap.get(bucket);
                AWS.debugMsg("is taskTracker null: %b", taskTracker == null);
                if (taskTracker == null) {
                    AWS.errorMsg("No task tracker for ID: %s", bucket);
                } else {
                    taskTracker.addResultToList(operation + ": " + pdfUrl + " " + outputUrlOrErrorMsg);
                    AWS.debugMsg("Added result to task tracker %s for: %s", bucket, pdfUrl);
                    if (taskTracker.isAllCompleted()) {
                        AWS.debugMsg("All tasks completed for input file: %s", taskTracker.getInputFileUrl());
                        workersMessageProcessorService.submit(() -> createAndSendSummaryFile(taskTracker, bucket));
                    }
                }
            }
            AWS.debugMsg("Successfully processed and deleted worker message from queue");
        } catch (Exception e) {
            AWS.errorMsg("Error processing worker response: %s", e.getMessage());
        }
    }

    private static void createAndSendSummaryFile(TaskTracker taskTracker, String bucketName) {
        AWS.debugMsg("Creating summary file for: %s", taskTracker.getInputFileUrl());
        try {
            File summaryFile = File.createTempFile("summary", ".txt");
            String summaryKey = "summaries/" + summaryFile.getName();

            try (PrintWriter writer = new PrintWriter(summaryFile)) {
                ConcurrentLinkedQueue<String> resultsList = taskTracker.getResultsList();
                for (String result : resultsList) {
                    writer.println(result);
                }
            }

            String summaryFileUrl = aws.uploadFileToS3(bucketName, summaryKey, summaryFile);
            AWS.debugMsg("Uploaded summary file to S3: %s", summaryFileUrl);
            aws.sendMessageToQueue(managerToLocalAppQueueUrl, summaryFileUrl);
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException ex) {
//                AWS.errorMsg("Thread interrupted. Message: %s", ex.getMessage());
//            }
            Files.delete(summaryFile.toPath());
            AWS.debugMsg("Summary file processed and cleaned up successfully");
            TasksMap.remove(bucketName);
            AWS.debugMsg("Deleted task Tracker for ID: %s", bucketName);
        } catch (Exception e) {
            AWS.errorMsg("Error creating/uploading summary file: %s", e.getMessage());
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
                        "Worker status - Pending tasks: %d, Current workers: %d, Required workers: %d, Workers to start: %d",
                        totalPendingTasks, currentWorkers, requiredWorkers, workersToStart);

                if (workersToStart > 0) {
                    AWS.debugMsg("Starting %d new worker instance(s)", workersToStart);
                    if (workerSemaphore.tryAcquire(workersToStart)) {
                        try {
                            aws.startWorkerInstances(workersToStart);
                            AWS.debugMsg("Successfully started %d worker instances", workersToStart);
                        } catch (Exception e) {
                            AWS.errorMsg("Failed to start worker instances: %s", e.getMessage());
                            workerSemaphore.release(workersToStart);
                        }
                    }
                }
            } else if (totalPendingTasks == 0 && System.currentTimeMillis() - lastWorkerResponse.get() >= MAX_TIME_WAITING){
                aws.terminateAllRunningWorkerInstances();
                workerSemaphore.release(MAX_WORKERS - workerSemaphore.availablePermits());
            }
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            AWS.errorMsg("Worker management thread interrupted: %s", e.getMessage());
        } catch (Exception e) {
            AWS.errorMsg("Error in worker management: %s", e.getMessage());
        }
    }

    public static void shutdownAndCleanup() {
        AWS.debugMsg("Starting shutdown sequence");

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
            AWS.debugMsg("All threads terminated successfully");
        } catch (InterruptedException e) {
            AWS.debugMsg("Thread termination interrupted: %s", e.getMessage());
            localAppMessageProcessorService.shutdownNow();
            workersMessageProcessorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        AWS.debugMsg("Terminating all worker instances");
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

        AWS.debugMsg("Cleanup completed successfully");
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
            AWS.errorMsg("Error during queue cleanup: %s", e.getMessage());
        }
    }

    private static void waitForQueuesToEmpty() {
        AWS.debugMsg("Waiting for queues to be empty before deletion...");

        boolean queuesEmpty = false;
        long waitedTime = 0;
        long startingTime = System.currentTimeMillis();

        while (!queuesEmpty && waitedTime < MAX_TIME_WAITING) {
            waitedTime = System.currentTimeMillis() - startingTime;
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
}
