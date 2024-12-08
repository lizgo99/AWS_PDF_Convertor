import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;

public class Worker {

    final static AWS aws = AWS.getInstance();


    public static void main(String[] args) {

        File currentDir = new File(".");
        AWS.debugMsg("Worker: Current directory: %s", currentDir.getAbsolutePath());
        AWS.debugMsg("Worker: Directory writable: %b", currentDir.canWrite());

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
            if (parts.length != 3) {
                AWS.errorMsg("%s - Invalid format", messageBody);
                break;
            }
            String operation = parts[0];
            String pdfUrl = parts[1];
            String bucketName = parts[2];
            AWS.debugMsg("PDF URL: %s", pdfUrl);
            String fileName = pdfUrl.substring(pdfUrl.lastIndexOf('/') + 1, pdfUrl.lastIndexOf('.'));
            try {
                File outputFile = null;
                String outputFileName = "";
                switch (operation) {
                    case "ToImage":
                        outputFileName = fileName + ".png";
                        outputFile = new File(outputFileName);
                        AWS.debugMsg("Worker: Starting PNG conversion for %s", pdfUrl);
                        Converter.toImage(pdfUrl, outputFile.getPath());
                        AWS.debugMsg("Worker: Finished PNG conversion. File exists: %b, Size: %d",
                                outputFile.exists(), outputFile.length());
                        break;
                    case "ToText":
                        outputFileName = fileName + ".txt";
                        outputFile = new File(outputFileName);
                        AWS.debugMsg("Worker: Starting TXT conversion for %s", pdfUrl);
                        Converter.toText(pdfUrl, outputFile.getPath());
                        AWS.debugMsg("Worker: Finished TXT conversion. File exists: %b, Size: %d",
                                outputFile.exists(), outputFile.length());
                        break;
                    case "ToHTML":
                        outputFileName = fileName + ".html";
                        outputFile = new File(outputFileName);
                        AWS.debugMsg("Worker: Starting HTML conversion for %s", pdfUrl);
                        Converter.toHTML(pdfUrl, outputFile.getPath());
                        AWS.debugMsg("Worker: Finished HTML conversion. File exists: %b, Size: %d",
                                outputFile.exists(), outputFile.length());

                        break;
                    default:
                        AWS.errorMsg("Invalid operation: %s", operation);
                        break;
                }
                if (outputFile == null) {
                    AWS.errorMsg("Output file is null");
                    continue;
                }

                String s3Key = "outputs/" + outputFileName;
                String outputUrl = aws.uploadFileToS3(bucketName, s3Key, outputFile);
                AWS.debugMsg("Uploaded to S3: %s", outputUrl);
                outputUrl = aws.getPublicFileUrl(bucketName, s3Key);
                aws.sendMessageToQueue(WorkersToManagerQueueUrl, operation + "\t" + pdfUrl + "\t" + outputUrl + "\t" + bucketName);
                AWS.debugMsg(String.format("Sent to Manager: %s\t%s\t%s\t%s" ,pdfUrl, outputUrl, operation,bucketName));
                aws.deleteMessageFromQueue(ManagerToWorkersQueueUrl, receiptHandle);

                // Clean up the local file after upload
                if (outputFile.exists()) {
                    outputFile.delete();
                }

            } catch (Exception e) {
//                String errorMessage = e.getMessage();
//                String fullMsg = String.format("%s\t%s\tcaused an exception during the conversion: %s", operation, pdfUrl, e.getMessage());
                AWS.errorMsg("%s\t%s\t caused an exception during the conversion: %s", operation, pdfUrl, e.getMessage());
                aws.sendMessageToQueue(WorkersToManagerQueueUrl, operation + "\t" + pdfUrl + "\t" + e.getMessage() + "\t" + bucketName);
                aws.deleteMessageFromQueue(ManagerToWorkersQueueUrl, receiptHandle);
            }
        }
    }

}