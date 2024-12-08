import org.apache.hc.client5.http.fluent.Request;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class Converter {

    public static void toImage(String pdfUrl, String outputPath) {
        // Fetch the content as an InputStream
        try (InputStream inputStream = Request.get(pdfUrl).execute().returnContent().asStream()) {
            // Process the InputStream as a PDF
            try (PDDocument document = PDDocument.load(inputStream)) {
                PDFRenderer pdfRenderer = new PDFRenderer(document);
                BufferedImage image = pdfRenderer.renderImageWithDPI(0, 300);
                ImageIO.write(image, "PNG", new File(outputPath));
            } catch (IOException e) {
                AWS.errorMsg("toImage: %s caused an exception during PDF operations: %s", pdfUrl, e.getMessage());
            }
        } catch (IOException e) {
            AWS.errorMsg("toImage: %s caused an exception during the HTTP request: %s", pdfUrl, e.getMessage());
        }
    }

    public static void toHTML(String pdfUrl, String outputFilePath) {
        try (InputStream inputStream = Request.get(pdfUrl).execute().returnContent().asStream()) {
            try (PDDocument document = PDDocument.load(inputStream)) {
                PDFText2HTML pdfStripper = new PDFText2HTML();
                pdfStripper.setStartPage(1);
                pdfStripper.setEndPage(1);

                java.io.StringWriter stringWriter = new java.io.StringWriter();
                pdfStripper.writeText(document, stringWriter);
                String content = stringWriter.toString();

                try (java.io.FileWriter writer = new java.io.FileWriter(outputFilePath)) {
                    writer.write(content);
                    writer.flush();
                }

//                File outputFile = new File(outputFilePath);
//                AWS.debugMsg("Converter: Wrote HTML file. Exists: %b, Size: %d",
//                        outputFile.exists(), outputFile.length());

                // pdfStripper.writeText(document, new java.io.FileWriter(outputFilePath));
            } catch (Exception e) {
                 AWS.errorMsg("toHTML: %s caused an exception during PDF operations: %s" , pdfUrl , e.getMessage());
            }
        } catch (Exception e) {
             AWS.errorMsg("toHTML: %s caused an exception during the HTTP request: %s" , pdfUrl , e.getMessage());


        }
    }

    public static void toText(String pdfUrl, String outputFilePath) {
        try (InputStream inputStream = Request.get(pdfUrl).execute().returnContent().asStream()) {
            try (PDDocument document = PDDocument.load(inputStream)) {
                PDFTextStripper pdfStripper = new PDFTextStripper();
                pdfStripper.setStartPage(1);
                pdfStripper.setEndPage(1);
                String text = pdfStripper.getText(document);
                File outputFile = new File(outputFilePath);
                try (java.io.FileWriter writer = new java.io.FileWriter(outputFile)) {
                    writer.write(text);
                } catch (IOException e) {
                    AWS.errorMsg("toText: %s caused an exception during writing to file:", e.getMessage());
                }
            } catch (Exception e) {
                AWS.errorMsg("toText: %s caused an exception during PDF operations: %s", pdfUrl, e.getMessage());
            }
        } catch (Exception e) {
            AWS.errorMsg("toText: %s caused an exception during the HTTP request: %s", pdfUrl, e.getMessage());
        }
    }

    public static void main(String[] args) {
        // toImage("https://www.bbc.co.uk/schools/religion/worksheets/pdf/judaism_passover_whatis.pdf",
        // "output_image.png");
        // toImage("http://www.jewishfederations.org/local_includes/downloads/39497.pdf",
        // "output_image.png");
        //
        // toHTML("http://www.st.tees.org.uk/assets/Downloads/Passover-service.pdf",
        // "output.html");
        toHTML("http://www.rabbinicalassembly.org/sites/default/files/public/jewish-law/holidays/pesah/why-do-we-sing-the-song-of-songs-on-passover.pdf",
                "output_html.html");
        //
        //
        // toText("http://www.chabad.org/media/pdf/42/kUgi423322.pdf",
        // "output_text.txt");
        // toText("http://yahweh.com/pdf/Booklet_Passover.pdf", "output_text.txt");
        // toText("http://www.barrylou.com/books/TellingTheStoryInside.pdf",
        // "output_text.txt");

        // for this url - http://www.barrylou.com/books/TellingTheStoryInside.pdf It
        // seems that page 1 and 2 are combined ? when printing page 2 it prints the
        // text of both pages

    }

}