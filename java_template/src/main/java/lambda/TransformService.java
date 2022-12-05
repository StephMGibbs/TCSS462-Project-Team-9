package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import saaf.*;



/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class TransformService implements RequestHandler<Request, HashMap<String, Object>> {
    
    String filename = "";
    String bucketname = "";
    
    // Collect inital data.
    Inspector inspector = new Inspector();

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        
        // inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        filename = request.getFilename();
        bucketname = request.getBucketname();

        //get object file using source bucket and srcKey name
         
        try {
            S3Object s3Object = s3Client.getObject(bucketname, filename);
            //S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));
            List<List<String>> lines = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                lines.add(Arrays.asList(values));
            }
            transformCSV(lines);
        } catch (IOException e) {
            Logger.getLogger(TransformService.class.getName()).log(Level.SEVERE, null, e);
        }
	
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private void transformCSV(List<List<String>> lines) {
        List<String> orderID = new ArrayList<>(lines.size());
        
        // Remove duplicate data identified by [Order ID]. Any record having an a duplicate [Order ID] that
        // has already been processed will be ignored.
        lines = lines.stream().filter(row -> {
            // Get the order id at index = 6
            String id = row.get(6);
            if (!orderID.contains(id)) {
                orderID.add(id);
                return true;
            }
            if (orderID.contains("Order ID")) {
                System.out.println("Found a header " + row.toString());
            }
            return false;
        }).collect(Collectors.toList());
        
        lines = lines.stream().map(row -> {
            try {
                row = new ArrayList(row.stream().map(x -> {x = x.replaceAll("\'", " ").replaceAll("[^\\x20-\\x7e]", "");
                return new String(x.getBytes(StandardCharsets.US_ASCII), StandardCharsets.US_ASCII);}).collect(Collectors.toList()));
                // Get the order priority
                String priority = String.valueOf(row.get(4));
                // Get the order region
		String region = String.valueOf(row.get(0));
		System.out.println("For " + region + " compare value : " + "Region".equals(region));
		switch (region) {
                    case "Region": {
                           
                        // Add column [Order Processing Time] column that stores an integer value representing the
                        // number of days between the [Order Date] and [Ship Date]
                        row.add("Order Processing Time");
                        
                        // Add a [Gross Margin] column. 
                        row.add("Gross Margin");
                        
			break;
                    }
                    default: {
                        
                        // Get the ship and order date
                        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
			Date shipDate = sdf.parse(row.get(7));
                        Date orderDate = sdf.parse(row.get(5));
                        
                        // Calculate the order processing time
                        long days = (shipDate.getTime() - orderDate.getTime()) / (1000 * 60 * 60 * 24);
                        
                        // Add the processing time to the "Order Processing Time" column
			row.add(String.valueOf(days));
                        
                        // Calculate a percentage of the Gross Margin: The Gross Margin Column is a percentage calculated using the
                        // formula: [Total Profit] / [Total Revenue]. It is stored as a floating point value (e.g 0.25 for 25%
                        // profit).
                        float grossMargin = Float.parseFloat(row.get(13)) / Float.parseFloat(row.get(11));
                        
                        // Add the gross margin to the Gross Margin" column
                        row.add(String.valueOf((grossMargin)));
                        
                        
                        
                        
                        // Transform [Order Priority] column:
                        // L to "Low"
                        // M to "Medium"
                        // H to "High"
                        // C to "Critical"
                        switch (priority) {
                            case "L":
                                row.set(4, "Low");
                                break;
                            case "M":
                                row.set(4, "Medium");
                                break;
                            case "H":
                                row.set(4, "High");
                                break;
                            case "C":
                                row.set(4, "Critical");
                                break;
                            default:
                                break;
                        }
                    }
		}
            } catch (NumberFormatException | ParseException e) {
		return null;
            }
            return row;
        }).filter(x -> x != null).collect(Collectors.toList()); 
        
        writeCSV(lines);
    }

    private void writeCSV(List<List<String>> lines) {
        StringWriter stringWriter = new StringWriter();
        
        // Write the data in the csv file format
	for(List<String> line: lines) {
            int i = 0;
            for (String value: line) {
                stringWriter.append(value);
                if (i++ != line.size() - 1)
                    stringWriter.append(',');
            }
            stringWriter.append("\n");
        }

	byte[] bytes = stringWriter.toString().getBytes(StandardCharsets.UTF_8);
	InputStream inputStream = new ByteArrayInputStream(bytes);
	ObjectMetadata objectMetadata = new ObjectMetadata();
	objectMetadata.setContentLength(bytes.length);
        objectMetadata.setContentType("text/plain");

        // Create new file on S3
	AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        String result = "Transform-" + filename;
	s3Client.putObject(bucketname, result, inputStream, objectMetadata);
        
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: " + bucketname + ", filename: Transform" + result + ", size: " + bytes.length);
       
        inspector.consumeResponse(response);
    }
}
