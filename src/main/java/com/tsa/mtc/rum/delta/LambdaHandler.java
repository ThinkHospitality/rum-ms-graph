package com.tsa.mtc.rum.delta;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.JsonObject;
import com.microsoft.graph.auth.publicClient.UsernamePasswordProvider;
import com.microsoft.graph.models.extensions.Event;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.options.HeaderOption;
import com.microsoft.graph.options.Option;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IEventDeltaCollectionPage;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class LambdaHandler implements RequestHandler<Object, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LambdaHandler.class);
    private final static String FILE_LOCATION = "/tmp/";
    private final static String CSV_EXTENSION = ".csv";
    private final static String TXT_EXTENSION = ".txt";
    private final static String BUCKET_PREFIX = "RUM-CSV-data/";
    private final static int PAGE_SIZE = 200;
    private static final String[] CSV_COLUMNS = new String[]
            {
                    "appointmentId", "hotelId", "hotelName", "opportunityId", "userId", "activityType", "startDateTime",
                    "endDateTime", "appointmentStatus", "durationMins", "durationDays", "durationHours",
                    "isBillable", "location", "activityDetails", "notes", "isTrainerLocal", "originalStartDate",
                    "originalEndDate", "createdBy", "createdDate", "modifiedBy", "modifiedDate", "subject", "eventType"
            };

    // Get bean for writing the logic into csv
    private static StatefulBeanToCsv<Appointment> getBeanWriter(FileWriter writer) {
        // Creating Mapping Strategy
        ColumnPositionMappingStrategy<Appointment> mappingStrategy = new ColumnPositionMappingStrategy<>();
        mappingStrategy.setType(Appointment.class);
        mappingStrategy.setColumnMapping(CSV_COLUMNS);

        // Creating StatefulBeanToCsv object
        StatefulBeanToCsvBuilder<Appointment> builder = new StatefulBeanToCsvBuilder<>(writer);
        return builder
                .withMappingStrategy(mappingStrategy)
                .withSeparator('|')
                .withApplyQuotesToAll(false)
                .build();
    }

    // Start of execution of function
    @Override
    public String handleRequest(Object input, Context context) {

        LOGGER.info("Start time = " + System.currentTimeMillis());
        LOGGER.debug("ENVIRONMENT VARIABLES = " + System.getenv());

        Regions clientRegion = Regions.AP_SOUTHEAST_1;

        // Fetching environment variables for execution
        String clientId = System.getenv("clientId");
        String username = System.getenv("username");
        String password = System.getenv("password");
        String bucket = System.getenv("bucket");
        String deltaToken_key = System.getenv("deltaToken_key");
        String startDateTime = System.getenv("startDateTime");
        String endDateTime = System.getenv("endDateTime");

        LOGGER.debug("clientId = " + clientId);
        LOGGER.debug("username = " + username);
        LOGGER.debug("password = " + password);
        LOGGER.debug("bucket = " + bucket);

        // Building S3 client for java
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .build();

        // Creating auth provider for MS graph api
        UsernamePasswordProvider authProvider = new UsernamePasswordProvider(
                clientId,
                Collections.singletonList("https://graph.microsoft.com/.default"),
                username,
                password);

        FileWriter writer = null;
        String deltaToken = null;
        String currentTime = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String csvFileName = "Appointments_" + currentTime + CSV_EXTENSION;
        String csvFilePath = FILE_LOCATION + csvFileName;
        String deltaFileName = "delta_" + currentTime + TXT_EXTENSION;

        try {
            writer = new FileWriter(csvFilePath);
            StatefulBeanToCsv<Appointment> csvWriter = getBeanWriter(writer);

            IGraphServiceClient graphClient = GraphServiceClient
                    .builder()
                    .authenticationProvider(authProvider)
                    .buildClient();

            // Check if delta token is already present, so that it can be used,
            // else full data will be fetched for given date range
            try {
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, BUCKET_PREFIX + deltaToken_key));
                InputStream objectData = s3Object.getObjectContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
                if (reader.ready()) {
                    deltaToken = reader.readLine();
                }
                LOGGER.info("Existing deltaToken = " + deltaToken);
            } catch (Exception e) {
                LOGGER.error("Exception Occurred while getting delta token");
                e.printStackTrace();
            }

            LOGGER.info("deltaToken before request = " + deltaToken);

            LinkedList<Option> requestOptions = new LinkedList<>();
            requestOptions.add(new HeaderOption("Prefer", "odata.maxpagesize=" + PAGE_SIZE));
            if (deltaToken != null) {
                requestOptions.add(new QueryOption("$deltatoken", deltaToken));
            } else {
                requestOptions.add(new QueryOption("startDateTime", startDateTime));
                requestOptions.add(new QueryOption("endDateTime", endDateTime));
            }

            IEventDeltaCollectionPage calendarViewDelta = graphClient.me().calendarView()
                    .delta()
                    .buildRequest(requestOptions)
                    .get();

            JsonObject rawObject = calendarViewDelta.getRawObject();

            String nextLink = Utilities.getLink(rawObject, "@odata.nextLink");
            String deltaLink = Utilities.getLink(rawObject, "@odata.deltaLink");

            LOGGER.info("nextLink = " + nextLink);
            LOGGER.info("deltaLink = " + deltaLink);

            List<Event> currentPage = calendarViewDelta.getCurrentPage();
            List<Appointment> appointmentList = Utilities.populateAppointmentData(currentPage);
            csvWriter.write(appointmentList);

            while (null == deltaLink && nextLink != null) {
                requestOptions = new LinkedList<>();
                requestOptions.add(new HeaderOption("Prefer", "odata.maxpagesize=" + PAGE_SIZE));
                requestOptions.add(new QueryOption("$skiptoken", Utilities.getToken(nextLink, "$skiptoken=")));

                calendarViewDelta = graphClient.me().calendarView()
                        .delta()
                        .buildRequest(requestOptions)
                        .get();

                rawObject = calendarViewDelta.getRawObject();

                nextLink = Utilities.getLink(rawObject, "@odata.nextLink");
                deltaLink = Utilities.getLink(rawObject, "@odata.deltaLink");

                LOGGER.info("loop nextLink = " + nextLink);
                LOGGER.info("loop deltaLink = " + deltaLink);

                currentPage = calendarViewDelta.getCurrentPage();
                appointmentList = Utilities.populateAppointmentData(currentPage);
                csvWriter.write(appointmentList);
            }

            deltaToken = Utilities.getToken(deltaLink, "$deltatoken=");

            LOGGER.info("Delta Link for next round = " + deltaLink);
            LOGGER.info("Delta Token for next round = " + deltaToken);

        } catch (CsvRequiredFieldEmptyException | CsvDataTypeMismatchException | IOException e) {
            LOGGER.error("Exception Occurred in main");
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
        	
        	// Writing delta token for next call
            s3Client.putObject(bucket, BUCKET_PREFIX + deltaToken_key, deltaToken);
            s3Client.putObject(bucket, BUCKET_PREFIX + deltaFileName, deltaToken);
            //crosschecking if file is written in S3 properly for timimg issues...
        /*	S3Object s1Object  = s3Client.getObject(new GetObjectRequest(bucket, BUCKET_PREFIX + deltaToken_key)); //deltafile.txt
        	InputStream objectData = s1Object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
            if (reader.ready()) {
                 deltaToken1 = reader.readLine();
            }
            S3Object s2Object = s3Client.getObject(new GetObjectRequest(bucket, BUCKET_PREFIX + deltaFileName));  // delta file with timeStamp
            InputStream objectData1 = s2Object.getObjectContent();
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(objectData1));
            if (reader.ready()) {
                 deltaToken2 = reader1.readLine();
            }
        	 if(deltaToken1!=null && deltaToken2!= null && deltaToken1.equals(deltaToken2)) {
        		 System.out.println("Tokens are equal are written correctly in s3....");
        	 }else if(deltaToken1==null) {
        		 System.out.println("Delta file with file name delta.txt is null....");
        	 }else {
        		 System.out.println("Delta file with file name delta_Timestamp.txt is null....");
        	 }*/
            
            // Copying csv file from lambda tmp folder to S3 bucket
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, BUCKET_PREFIX + csvFileName, new File(csvFilePath));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            putObjectRequest.setMetadata(metadata);
            s3Client.putObject(putObjectRequest);

            

        } catch (AmazonServiceException e) {
            e.printStackTrace();
            LOGGER.error("AmazonServiceException occurred.");
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Exception while writing new delta token in bucket.");
        }

        LOGGER.info("End time = " + System.currentTimeMillis());

        return "CSV File generated Successfully.";
        
    }
}
