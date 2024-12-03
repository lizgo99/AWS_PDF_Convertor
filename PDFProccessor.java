import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.http.NoHttpResponseException;

import java.io.*;
import java.net.URL;

public class PDFProccessor {
    public static void processFile(String inputFilePath, String outputFilePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
             PrintWriter writer = new PrintWriter(new FileWriter(outputFilePath))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length != 2) {
                    writer.println(line + " - Invalid format");
                    continue;
                }

                String operation = parts[0];
                String pdfUrl = parts[1];

                try {
                    validateUrl(pdfUrl);
                    writer.println(operation + ": " + pdfUrl + " - Valid");
                } catch (Exception e) {
                    writer.println(operation + ": " + pdfUrl + " - " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
        }
    }

    public static void validateUrl(String url) throws Exception {
        try {
            Request.get(url).execute().returnContent().asStream(); // Attempt to access URL content
        } catch (NoHttpResponseException e) {
            throw new Exception("HTTP Error: " + e.getMessage() + " cause- " + e.getCause());
        } catch (IOException e) {
            throw new Exception("Invalid URL or cannot connect");
        }
    }

    public static void main(String[] args) {
        String inputFilePath = "input-sample-1.txt";  // Path to the input file
        String outputFilePath = "output-results-1.txt"; // Path to the output file

        processFile(inputFilePath, outputFilePath);
        System.out.println("Processing complete. Results saved to " + outputFilePath);

//        try {
////            Request.get("http://www.crumbs.com/media/inline/HolidayMenu_Passover_10.pdf").execute();;
//            validateUrl("http://www.crumbs.com/media/inline/HolidayMenu_Passover_10.pdf");
////            validateUrl("https://www.jewishlibraries.org/main/Portals/0/AJL_Assets/documents/bib_bank/PassoverResources.pdf\n");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }
}
