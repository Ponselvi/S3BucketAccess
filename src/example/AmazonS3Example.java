package example;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


public class AmazonS3Example {
	private static final String SUFFIX = "/";

	public static void main(String[] args) {
		try {
			// credentials object identifying user for authentication
			// user must have AWSConnector and AmazonS3FullAccess for
			// this example to work
			
			 
			AWSCredentials credentials = new BasicAWSCredentials("AccessId",
					"SecretId");


			// create a client connection based on credentials
			AmazonS3 s3client = new AmazonS3Client(credentials);

			// create bucket - name must be unique for all S3 users
			/*
			 * String bucketName = "s3-example-bucket";
			 * s3client.createBucket(bucketName);
			 */

			// list buckets
			for (Bucket bucket : s3client.listBuckets()) {
				System.out.println(" - " + bucket.getName());
			}
			String bucketName = "jarfilecheck";
			// create folder into bucket
			String folderName = "testfolderJDK";
			createFolder(bucketName, folderName, s3client);

			// upload file to folder and set it to public
			// String fileName = folderName + SUFFIX + "test.txt";
			String fileName = "test.txt";
			s3client.putObject(
					new PutObjectRequest(bucketName, fileName, new File("C:\\Users\\Desktop\\test.txt"))
							.withCannedAcl(CannedAccessControlList.PublicRead));

			GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, "test.txt");
			//rangeObjectRequest.setRange(0, 10);
			S3Object objectPortion = s3client.getObject(rangeObjectRequest);
			
			System.out.println("Printing bytes retrieved.");
			displayTextInputStream(objectPortion.getObjectContent());
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which" + " means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means" + " the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}catch (IOException ioe) {
			System.out.println("IOException"+ioe);
		}
		//deleteFolder(bucketName, folderName, s3client);

		// deletes bucket
		 //s3client.deleteBucket(bucketName);
		
	}

	public static void createFolder(String bucketName, String folderName, AmazonS3 client) {
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);

		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + SUFFIX, emptyContent,
				metadata);

		// send request to S3 to create folder
		client.putObject(putObjectRequest);
	}

	/**
	 * This method first deletes all the files in given folder and than the
	 * folder itself
	 */
	/*
	 * public static void deleteFolder(String bucketName, String folderName,
	 * AmazonS3 client) { List fileList = client.listObjects(bucketName,
	 * folderName).getObjectSummaries(); for (S3ObjectSummary file : fileList) {
	 * client.deleteObject(bucketName, file.getKey()); }
	 * client.deleteObject(bucketName, folderName); }
	 */

	private static void displayTextInputStream(InputStream input) throws IOException {
		// Read one text line at a time and display.
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;

			System.out.println("    " + line);
		}
		System.out.println();
	}
}