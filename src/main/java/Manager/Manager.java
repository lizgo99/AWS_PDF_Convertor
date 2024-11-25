package Manager;

import AWS.AWS;
import LocalApp.LocalApp;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;

import static java.lang.Math.min;


public class Manager {

    final static AWS aws = AWS.getInstance();

    static final int MAX_INSTANCES = 9;  // decide the number of workers
    static int numberOfActiveWorkers = 0;
    static boolean shouldTerminate = false;
    static int taskCounter = 0;

    static String ManagerToWorkersQueueUrl = "";
    static String WorkersToManagerQueueUrl = "";
    static String LocalAppToManagerQueueUrl = "";
    static String ManagerToLocalAppQueueUrl = "";


    public static void main(String[] args) {
        // create the queues
        ManagerToWorkersQueueUrl = aws.createSqsQueue("ManagerToWorkersQueue");
        WorkersToManagerQueueUrl = aws.createSqsQueue("WorkersToManagerQueue");
        // connect to the existing queues
        LocalAppToManagerQueueUrl = aws.connectToQueueByName("LocalAppToManagerQueue");
        ManagerToLocalAppQueueUrl = aws.connectToQueueByName("ManagerToLocalAppQueue");

        while (!shouldTerminate) {

            reciveAndParseMsgFromLocalApp(LocalAppToManagerQueueUrl);
            reciveAndParseMsgFromWorker(WorkersToManagerQueueUrl);


        }


//            if (message == null) {
//                shouldTerminate = true;  // check when is a message null? do we neet to terminate if the message is null?
//                break;
//            }


        //END
        //terminate all the workers , delete the queues and the buckets if were created , terminate manager

    }
    public static void reciveAndParseMsgFromWorker (String queueURL) {
        Message message = aws.getMessageFromQueue(WorkersToManagerQueueUrl);
        String messageBody = message.body();


    }

    public static void reciveAndParseMsgFromLocalApp (String queueURL) {
        // get message from LocalApp
        Message message = aws.getMessageFromQueue(LocalAppToManagerQueueUrl);
        String messageBody = message.body();
        if (messageBody.equals("terminate")) {
            shouldTerminate = true;
            return;
        }
        else{
            // get the file from the message
            String[] parts = messageBody.split("\t");
            if (parts.length != 1) {
                System.out.println(messageBody + " - Invalid format");
                return;
            }
            String inputFileName = parts[0];
            // download the file from S3
            File inputFile = new File(inputFileName);
            aws.downloadFileFromS3(inputFileName, inputFile);

            // for each Url in file send message to the workers queue
            sendTasksToQueue(ManagerToWorkersQueueUrl);
        }
        aws.deleteMessageFromQueue(LocalAppToManagerQueueUrl, message.receiptHandle());

    }

    public static void sendTasksToQueue (String queueURL) {
        File inputFile = new File("input.txt");
        aws.downloadFileFromS3(LocalApp.inputFileName, inputFile);
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile.getPath()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) {
                    System.out.println(line + " - Invalid format");
                    continue;
                }

                String operation = parts[0];
                String pdfUrl = parts[1];
                aws.sendMessageToQueue(queueURL, line);
                taskCounter++;
            }
        } catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
        }
    }



    public static void createWorkerNodes (int tasksPerWorker, String queueURL) {

        if (numberOfActiveWorkers < taskCounter && numberOfActiveWorkers < MAX_INSTANCES)  {
            int numberOfInstances = min(taskCounter - numberOfActiveWorkers, tasksPerWorker - numberOfActiveWorkers);
            for (int i = 0; i < numberOfInstances; i++){
                // TODO: add worker script
                aws.createEC2("worker script", "worker", 1);
            }
            numberOfActiveWorkers += numberOfInstances;
        }
    }
}
