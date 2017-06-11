package com.amazonaws.samples;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClientBuilder;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;

/**
 * Welcome to your new AWS Java SDK based project!
 *
 * This class is meant as a starting point for your console-based application that
 * makes one or more calls to the AWS services supported by the Java SDK, such as EC2,
 * SimpleDB, and S3.
 *
 * In order to use the services in this sample, you need:
 *
 *  - A valid Amazon Web Services account. You can register for AWS at:
 *       https://aws-portal.amazon.com/gp/aws/developer/registration/index.html
 *
 *  - Your account's Access Key ID and Secret Access Key:
 *       http://aws.amazon.com/security-credentials
 *
 *  - A subscription to Amazon EC2. You can sign up for EC2 at:
 *       http://aws.amazon.com/ec2/
 *
 *  - A subscription to Amazon SimpleDB. You can sign up for Simple DB at:
 *       http://aws.amazon.com/simpledb/
 *
 *  - A subscription to Amazon S3. You can sign up for S3 at:
 *       http://aws.amazon.com/s3/
 */
public class AwsConsoleApp {

    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (/Users/zer0x/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonEC2      ec2;
    static AmazonS3       s3;
    static AmazonSimpleDB sdb;
    
    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    private static void init() throws AmazonClientException {

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/Users/zer0x/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/Users/zer0x/.aws/credentials), and is in valid format.",
                    e);
        }
        ec2 = AmazonEC2ClientBuilder.standard().withCredentials(
        		new AWSStaticCredentialsProvider(credentials)).withRegion(
        				Regions.US_WEST_2).build();
        s3 = AmazonS3ClientBuilder.standard().withCredentials(
        		new AWSStaticCredentialsProvider(credentials)).withRegion(
        				Regions.US_WEST_2).build();
        sdb = AmazonSimpleDBClientBuilder.standard().withCredentials(
        		new AWSStaticCredentialsProvider(credentials)).withRegion(
        				Regions.US_WEST_2).build();
    }

    /**
     * Connects to the MySQL database hosted on AWS.
     * 
     * @return Connection object pointing to AWS
     */
    public static Connection connectJDBCToAWSEC2() {

        String PUBLIC_DNS = "jneidbal.cv4irfbbewqs.us-west-2.rds.amazonaws.com";
        String PORT = "3306";
        String DATABASE = "ebdb";
        String REMOTE_DATABASE_USERNAME = "remote";
        String DATABASE_USER_PASSWORD = "jneidbal";
        
        Connection connection = null;
    	
        System.out.println("----MySQL JDBC Connection Testing -------");
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your MySQL JDBC Driver?");
            e.printStackTrace();
            return connection;
        }

        System.out.println("MySQL JDBC Driver Registered!");

        try {
            connection = DriverManager.getConnection("jdbc:mysql://" +
            		PUBLIC_DNS + ":" + PORT + "/" + DATABASE,
            		REMOTE_DATABASE_USERNAME, DATABASE_USER_PASSWORD);
        } catch (SQLException e) {
            System.out.println("Connection Failed!:\n" + e.getMessage());
        }

        if (connection != null) {
            System.out.println("SUCCESS: take control of your AWS database now.");
        } else {
            System.out.println("FAILURE: failed to make connection.");
        }
        return connection;
    }

    /**
     * Queries the given MySQL table and outputs all of its records.
     * 
     * @param conn  Connection object pointing to the appropriate MySQL database
     * @param table a string representing the name of the SQL table
     */
    private static void runTestQuery(Connection conn, String table) {
    	ArrayList<String> columns = new ArrayList<String>();
        Statement statement = null;
        try {

            System.out.println("Executing statement: \"SELECT * FROM " +
            		table + "\":");
            statement = conn.createStatement();

            ResultSet rs = statement.executeQuery("SELECT * FROM " + table);
            
            ResultSetMetaData rsmd = rs.getMetaData();
            
            for(int i=1; i <= rsmd.getColumnCount(); i++) {
            	String column = rsmd.getColumnLabel(i);
            	columns.add(column);
            	System.out.print(column + "\t");
            }
            System.out.println();

            while (rs.next()) {
                for (Iterator<String> c = columns.iterator(); c.hasNext();) {
                	System.out.print(rs.getString(c.next()) + "\t");
                }
                System.out.println();
            }
            System.out.println();
            
            rs.close();
            statement.close();
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
            try {
                if (statement != null) statement.close();
            } catch (SQLException se2) {
            	se2.printStackTrace();
            }
        }
    }

    /**
     * Updates the DEALS_PER_CURRENCY table with the loaded values in the given
     * table.
     * 
     * @param conn  Connection object pointing to the appropriate MySQL database
     * @param table the name of the SQL table aggregate data from  
     * @throws SQLException @{link SQLException}
     */
    public static void UpdateDealAggregate(Connection conn, String table)
    		throws SQLException {
        String inClause = "";
        String insertQuery = "INSERT INTO DEALS_PER_CURRENCY (SELECT DISTINCT " +
        		"FROM_CURRENCY AS CURRENCY_ID, COUNT(*) AS COUNT_OF_DEALS, " +
        		"SUM(COALESCE(AMOUNT, 0.0)) AS SUM_OF_DEALS FROM " + table;
        String listQuery = "SELECT DISTINCT CURRENCY_ID FROM DEALS_PER_CURRENCY";
        Statement statement = null;
        try {
	        statement = conn.createStatement();
	        String updateQuery = "UPDATE DEALS_PER_CURRENCY d " +
	        		"INNER JOIN (SELECT DISTINCT FROM_CURRENCY, COUNT(*) AS C," +
	        		"SUM(COALESCE(AMOUNT, 0.0)) AS S FROM " + table +
	        		" GROUP BY FROM_CURRENCY) v ON v.FROM_CURRENCY = " +
	        		"d.CURRENCY_ID SET d.COUNT_OF_DEALS = v.C, d.SUM_OF_DEALS = v.S";
	        ResultSet results = statement.executeQuery(listQuery);
	        while (results.next()) {
	            if (inClause.length() > 0) inClause += ", ";
	            inClause += "'" + results.getString("CURRENCY_ID") + "'";
	        }
	        if (!inClause.equals("")) {
	            insertQuery += " WHERE FROM_CURRENCY NOT IN (" + inClause + ")";
	            updateQuery += " WHERE v.FROM_CURRENCY IN (" + inClause + ")";
	        }
	        insertQuery += " GROUP BY FROM_CURRENCY)";
	        statement.executeUpdate(insertQuery);
	        statement.executeUpdate(updateQuery);
	        statement.close();
	        System.out.println("Successfully updated the DEALS_PER_CURRENCY table.");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception {

        System.out.println("===========================================");
        System.out.println("Welcome to the AWS Java SDK!");
        System.out.println("===========================================");

        init();

        /*
         * Amazon EC2
         *
         * The AWS EC2 client allows you to create, delete, and administer
         * instances programmatically.
         *
         * In this sample, we use an EC2 client to get a list of all the
         * availability zones, and all instances sorted by reservation id.
         */
        try {
            DescribeAvailabilityZonesResult availabilityZonesResult =
            		ec2.describeAvailabilityZones();
            System.out.println("You have access to " +
            		availabilityZonesResult.getAvailabilityZones().size() +
            		" Availability Zones.");

            DescribeInstancesResult describeInstancesRequest =
            		ec2.describeInstances();
            List<Reservation> reservations =
            		describeInstancesRequest.getReservations();
            Set<Instance> instances = new HashSet<Instance>();

            for (Reservation reservation : reservations) {
                instances.addAll(reservation.getInstances());
            }

            System.out.println("You have " + instances.size() +
            		" Amazon EC2 instance(s) running.");
        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

        /*
         * Amazon SimpleDB
         *
         * The AWS SimpleDB client allows you to query and manage your data
         * stored in SimpleDB domains (similar to tables in a relational DB).
         *
         * In this sample, we use a SimpleDB client to iterate over all the
         * domains owned by the current user, and add up the number of items
         * (similar to rows of data in a relational DB) in each domain.
         */
        try {
            ListDomainsRequest sdbRequest =
            		new ListDomainsRequest().withMaxNumberOfDomains(100);
            ListDomainsResult sdbResult = sdb.listDomains(sdbRequest);

            int totalItems = 0;
            for (String domainName : sdbResult.getDomainNames()) {
                DomainMetadataRequest metadataRequest =
                		new DomainMetadataRequest().withDomainName(domainName);
                DomainMetadataResult domainMetadata =
                		sdb.domainMetadata(metadataRequest);
                totalItems += domainMetadata.getItemCount();
            }

            System.out.println("You have " + sdbResult.getDomainNames().size() +
            		" Amazon SimpleDB domain(s) containing a total of " +
            		totalItems + " items.");
        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

        /*
         * Amazon S3
         *
         * The AWS S3 client allows you to manage buckets and programmatically
         * put and get objects to those buckets.
         *
         * In this sample, we use an S3 client to iterate over all the buckets
         * owned by the current user, and all the object metadata in each
         * bucket, to obtain a total object and space usage count. This is done
         * without ever actually downloading a single object -- the requests
         * work with object metadata only.
         */
        try {
            List<Bucket> buckets = s3.listBuckets();

            long totalSize  = 0;
            int  totalItems = 0;
            for (Bucket bucket : buckets) {
                /*
                 * In order to save bandwidth, an S3 object listing does not
                 * contain every object in the bucket; after a certain point the
                 * S3ObjectListing is truncated, and further pages must be
                 * obtained with the AmazonS3Client.listNextBatchOfObjects()
                 * method.
                 */
                ObjectListing objects = s3.listObjects(bucket.getName());
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        totalSize += objectSummary.getSize();
                        /*System.out.println("\tobjectSummary[" +
                        		Long.toString(totalSize) + "] = " +
                        		objectSummary.toString());*/
                        totalItems++;
                    }
                    objects = s3.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            }

            System.out.println("You have " + buckets.size() +
            		" Amazon S3 bucket(s), containing " + totalItems +
            		" objects with a total size of " + totalSize +
            		" bytes.");
        } catch (AmazonServiceException ase) {
            /*
             * AmazonServiceExceptions represent an error response from an AWS
             * services, i.e. your request made it to AWS, but the AWS service
             * either found it invalid or encountered an error trying to execute
             * it.
             */
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            /*
             * AmazonClientExceptions represent an error that occurred inside
             * the client on the local host, either while trying to send the
             * request to AWS or interpret the response. For example, if no
             * network connection is available, the client won't be able to
             * connect to AWS to execute a request and will throw an
             * AmazonClientException.
             */
            System.out.println("Error Message: " + ace.getMessage());
        }
        
        Connection c = connectJDBCToAWSEC2();
        if (c != null) {
        	UpdateDealAggregate(c, "VALID_DEALS");
        	UpdateDealAggregate(c, "INVALID_DEALS");
	        for(String i : Arrays.asList("INVALID_DEALS", "LOADED_FILES",
	        		"VALID_DEALS", "DEALS_PER_CURRENCY")) {
	        	runTestQuery(c, i);
	        }
	        c.close();
        } else {
        	System.out.println("ERROR: failed to make a connection.");
        }
    }
}
