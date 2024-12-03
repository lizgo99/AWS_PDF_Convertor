package Worker;

import AWS.AWS;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;

public class Worker {

    final static AWS aws = AWS.getInstance();

    public static void main(String[] args) {

        String ManagerToWorkersQueueUrl = aws.connectToQueueByName("ManagerToWorkers");
        String WorkersToManagerQueueUrl = aws.connectToQueueByName("WorkersToManager");

        while (true) {
            Message message = aws.getMessageFromQueue(ManagerToWorkersQueueUrl);
            if (message == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    AWS.debugMsg("Thread interrupted while waiting for message");
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }
            aws.changeVisibilityTimeout(ManagerToWorkersQueueUrl, message.receiptHandle(), 60);
            String messageBody = message.body();
            String receiptHandle = message.receiptHandle();
            if (messageBody == null) {
                break;
            }
            String[] parts = messageBody.split("\t");
            if (parts.length != 2) {
                AWS.errorMsg(messageBody + " - Invalid format");
                break;
            }
            String pdfUrl = parts[1];
            AWS.debugMsg("PDF URL: " + pdfUrl);
            String operation = parts[0];
            String fileName = pdfUrl.substring(pdfUrl.lastIndexOf('/') + 1, pdfUrl.lastIndexOf('.'));
            try {
                File outputFile = null;
                String outputFileName = "";
                switch (operation) {
                    case "ToImage":
                        outputFileName = fileName + ".png";
                        outputFile = new File(outputFileName);
                        Converter.toImage(pdfUrl, outputFile.getPath());
                        break;
                    case "ToText":
                        outputFileName = fileName + ".txt";
                        outputFile = new File(outputFileName);
                        Converter.toText(pdfUrl, outputFile.getPath());
                        break;
                    case "ToHTML":
                        outputFileName = fileName + ".html";
                        outputFile = new File(outputFileName);
                        Converter.toHTML(pdfUrl, outputFile.getPath());
                        break;
                    default:
                        AWS.errorMsg("Invalid operation: " + operation);
                        break;
                }
                if (outputFile == null) {
                    AWS.errorMsg("Output file is null");
                    continue;
                }

                String s3Key = "outputs/" + outputFileName;
                String outputUrl = aws.uploadFileToS3(s3Key, outputFile);
                AWS.debugMsg("Uploaded to S3: " + outputUrl);
                aws.sendMessageToQueue(WorkersToManagerQueueUrl, operation + "\t" + pdfUrl + "\t" + outputUrl);
                AWS.debugMsg("Sent to Manager: " + pdfUrl + "\t" + outputUrl + "\t" + operation);
                aws.deleteMessageFromQueue(ManagerToWorkersQueueUrl, receiptHandle);

                // Clean up the local file after upload
                if (outputFile.exists()) {
                    outputFile.delete();
                }

            } catch (Exception e) {
                String errorMessage = operation + "\t" + pdfUrl + "\t" + " caused an exception during the conversion: "
                        + e.getMessage();
                AWS.errorMsg(errorMessage);
                aws.sendMessageToQueue(WorkersToManagerQueueUrl, errorMessage);
                aws.deleteMessageFromQueue(ManagerToWorkersQueueUrl, receiptHandle);
                continue;
            }
        }
    }

}
