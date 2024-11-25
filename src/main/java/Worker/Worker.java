package Worker;
import AWS.AWS;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;

public class Worker {

    final static AWS aws = AWS.getInstance();

    private static boolean shouldTerminate = false;


    public static void main(String[] args) {
//        A worker process resides on an EC2 node. Its life cycle is as follows:
//        Repeatedly:
//            ▪ Get a message from an SQS queue.
//            ▪ Download the PDF file indicated in the message.
//            ▪ Perform the operation requested on the file.
//            ▪ Upload the resulting output file to S3.
//            ▪ Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new image file, and the operation that was performed.
//            ▪ remove the processed message from the SQS queue.

        String ManagerToWorkersQueueUrl = aws.connectToQueueByName("ManagerToWorkersQueue");
        String WorkersToManagerQueueUrl = aws.connectToQueueByName("WorkersToManagerQueue");

        while (!shouldTerminate) {
            Message message = aws.getMessageFromQueue(ManagerToWorkersQueueUrl); // should be ManagerToWorkersQueueUrl
            if (message == null) {
                shouldTerminate = true;       // Good for now to break the loop. Should be changed to a condition that checks if the manager wants to terminate the workers
                break;
            }
            String messageBody = message.body();
            String receiptHandle = message.receiptHandle();
            if (messageBody == null) {
                break;
            }
            String[] parts = messageBody.split("\t");
            if (parts.length != 2) {
                System.out.println(messageBody + " - Invalid format");
                break;
            }
            String pdfUrl = parts[1];
            String operation = parts[0];
            String keyPath = pdfUrl.substring(pdfUrl.lastIndexOf('/') + 1, pdfUrl.lastIndexOf('.'));

            try {
                File outputFile = null;
                switch (operation) {
                    case "ToImage":
                        outputFile = new File(keyPath + ".png");
                        Converter.toImage(pdfUrl, outputFile.getPath());
                        break;
                    case "ToText":
                        outputFile = new File(keyPath + ".txt");
                        Converter.toText(pdfUrl, outputFile.getPath() + ".txt");
                        break;
                    case "ToHTML":
                        outputFile = new File(keyPath + ".html");
                        Converter.toHTML(pdfUrl, outputFile.getPath() + ".html");
                        break;
                    default:
                        System.out.println("Invalid operation: " + operation);
                        break;
                }
                if (outputFile == null) {
                    continue;
                }
                String outputUrl = aws.uploadFileToS3(keyPath, outputFile);
                aws.sendMessageToQueue(WorkersToManagerQueueUrl, pdfUrl + "\t" + outputUrl + "\t" + operation);
                aws.deleteMessageFromQueue(ManagerToWorkersQueueUrl, receiptHandle);



            } catch (Exception e) {
                System.err.println("Error processing task: " + e.getMessage());
            }
        }

    }

}
