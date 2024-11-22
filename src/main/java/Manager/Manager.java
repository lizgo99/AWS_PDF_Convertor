package Manager;

import AWS.AWS;
import LocalApp.LocalApp;

import java.io.*;


public class Manager {

    final static AWS aws = AWS.getInstance();
    static int numberOfActiveWorkers = 0;
    boolean shouldTerminate = false;


    public static void main(String[] args) {


        String queueName = "ManagerToWorkersQueue";
        String queueUrl = aws.createSqsQueue(queueName);
        // for each Url in file send message to the workers queue

        handTasks(1, queueUrl);




        //END
        //terminate all the workers , delete the queues and the buckets if were created , terminate manager

    }


    public static void handTasks (int tasksPerWorker, String queueURL) {
        File inputFile = new File("input.txt");
        aws.downloadFileFromS3(LocalApp.inputFileName, inputFile);
        int taskCounter = 0;
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

        if (numberOfActiveWorkers < taskCounter && numberOfActiveWorkers < 9)  {
            int numberOfInstances = min(taskCounter - numberOfActiveWorkers, tasksPerWorker - numberOfActiveWorkers);
            aws.createEC2("worker script", "worker", numberOfInstances);
            numberOfActiveWorkers += numberOfInstances;
        }
    }
}
