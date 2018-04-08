package com.cruise.plugins.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class CruiseS3Bucket {
	private AWSCredentials creds = null;
	private AmazonS3 s3client = null;
	private ObjectMetadata metadata = null;
	private String AccessKey = null;
	private String SecretKey = null;
/**
 * Creates a AWSCredential and AmazonS3 objects
 * @param accessKey
 * @param secretKey
 * @param region
 */
	public CruiseS3Bucket(String accessKey, String secretKey, String region, String endPoint) {
		try {
		AccessKey = accessKey;
		SecretKey = secretKey;
		creds = createCredentials(accessKey, secretKey);
		if(true==false) {
			//creds.setEndpoint("http");
		}
		if(null != creds) {
			s3client = createClient(creds, region, endPoint);
		}
		}catch(Exception e) {
			e.printStackTrace();
		}

	}
	/**
	 * Called during construction
	 * @param accessKey
	 * @param secretKey
	 * @return
	 */
	private AWSCredentials createCredentials(String accessKey, String secretKey) {
		AWSCredentials ret = null;
		try {
			ret = new BasicAWSCredentials(
					accessKey, 
					secretKey
					);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
	/**
	 * Called during construction
	 * @param credentials
	 * @param region
	 * @return
	 */
	private AmazonS3 createClient(AWSCredentials credentials, String region, String endPoint) {
		AmazonS3 ret = null;
		try {
			if(null == region) {
				region = Regions.US_WEST_2.toString();
			}
			if(null == endPoint) {
				ret = AmazonS3ClientBuilder
						.standard()
						.withCredentials(new AWSStaticCredentialsProvider(credentials))
						.withRegion(region)
						.build();
			}else {
				ret = AmazonS3ClientBuilder
						.standard()
						.withCredentials(new AWSStaticCredentialsProvider(credentials))
						.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint,region))
						.build();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}

		return ret;

	}
	/**
	 * Creates a new bucket
	 * @param bucketName
	 * @return
	 * @throws Exception
	 */
	public boolean createBucket(String bucketName)  throws Exception{
		boolean ret = false;

		if(s3client.doesBucketExist(bucketName)) {
			System.out.println("Bucket name is not available." + " Try again with a different Bucket name.");

		}else {

			s3client.createBucket(bucketName);
			ret = true;
		}

		return ret;
	}
	/**
	 * Lists all buckets 
	 * @return
	 * @throws Exception
	 */
	public synchronized List<Bucket> listBuckets()  throws Exception{
		List<Bucket> ret = null;

		ret = s3client.listBuckets();
		//for(Bucket bucket : ret) {
		//	ret.add(bucket);
		//}

		return ret;
	}
	/**
	 * removes a bucket
	 * @param bucketName
	 * @return
	 * @throws Exception
	 */
	public boolean deleteBucket(String bucketName) throws Exception {
		boolean ret = false;

		s3client.deleteBucket(bucketName);
		ret = true;

		return ret;
	}
	/**
	 * Retuns a stream for an object
	 * @param bucketName
	 * @param objectName
	 * @return
	 * @throws Exception
	 */
	public S3ObjectInputStream getObject(String bucketName, String objectName) throws Exception {
		S3ObjectInputStream ret = null;
		S3Object s3object = s3client.getObject(bucketName, objectName);
		ret = s3object.getObjectContent();
		return ret;
	}
	public String getObjectString(String bucketName, String objectName) throws Exception {
		StringBuffer result = new StringBuffer();
		S3ObjectInputStream ret = null;
		S3Object s3object = s3client.getObject(bucketName, objectName);
		BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
		String line;
		while((line = reader.readLine()) != null) {
		     result.append(line);
		}
		return result.toString();
	}
	/**
	 * Writes a string to an s3 object
	 * @param bucketName
	 * @param objectName
	 * @param out
	 * @return 
	 * @throws Exception
	 */
	public boolean putObjectString(String bucketName, String objectName, String out) throws Exception {
		s3client.putObject(
				bucketName, 
				objectName, 
				out
				);
		return true;
	}
	/**
	 * Writes a java File object to an s3 object
	 * @param bucketName
	 * @param objectName
	 * @param out
	 * @throws Exception
	 */
	public void putObjectFile(String bucketName, String objectName, File out) throws Exception {
		s3client.putObject(
				bucketName, 
				objectName, 
				out
				);
	}
	/**
	 * Writes an Inputstream to an s3 object. You can optionally populate this classes metadata object to be included. see getMetaData.
	 * @param bucketName
	 * @param objectName
	 * @param out
	 * @throws Exception
	 */
	public void putObjectStream(String bucketName, String objectName, InputStream out) throws Exception{

		s3client.putObject(
				bucketName, 
				objectName, 
				out,
				metadata
				);
	}
	/**
	 * Copies an object from the fromBucketName to the toBucketName. The name is unchanged.
	 * @param fromBucketName
	 * @param toBucketName
	 * @param objectName
	 * @return
	 * @throws Exception
	 */
	public boolean copyObject(String fromBucketName, String toBucketName, String objectName)  throws Exception{
		boolean ret = false;

		s3client.copyObject(
				fromBucketName, 
				objectName, 
				toBucketName, 
				objectName
				);
		ret = true;

		return ret;
	}
	/**
	 * Renames the S3 Object
	 * @param bucketName
	 * @param fromObjectName
	 * @param toObjectName
	 * @return
	 * @throws Exception
	 */
	public boolean renameObject(String bucketName, String fromObjectName, String toObjectName)  throws Exception{
		boolean ret = false;

		s3client.copyObject(
				bucketName, 
				fromObjectName, 
				bucketName, 
				toObjectName
				);
		ret = true;

		return ret;
	}
	public ObjectListing listAllObjects(String bucketName) throws Exception{
        ObjectListing ret = s3client.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName));
        return ret;
	}
	public ObjectListing listPrefixedObjects(String bucketName, String preFix) throws Exception {
        ObjectListing ret = s3client.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(preFix)
                );
        return ret;
	}
	/**
	 * Deletes the S3 Object
	 * @param bucketName
	 * @param objectName
	 * @return
	 * @throws Exception
	 */
	public boolean deleteObject(String bucketName, String objectName)  throws Exception{
		boolean ret = false;

		s3client.deleteObject(bucketName,objectName);
		ret = true;

		return ret;
	}
	/**
	 * Deletes all the Objects in the string[] objectNames
	 * @param bucketName
	 * @param objectNames
	 * @return
	 * @throws Exception
	 */
	public boolean deleteObjects( String bucketName, String[] objectNames)  throws Exception{
		boolean ret = false;

		DeleteObjectsRequest delObjReq = new DeleteObjectsRequest("baeldung-bucket")
				.withKeys(objectNames);
		s3client.deleteObjects(delObjReq);

		return ret;
	}
	/**
	 * If populated, this object will be used by the putStreamObject method.
	 * @return
	 * @throws Exception
	 */
	public ObjectMetadata getMetaData()  throws Exception{
		/*ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("format", "jpeg");
        metadata.setContentType("image/jpeg");*/
		if(null == metadata) {
			metadata = new ObjectMetadata();
		}
		return metadata;
	}
	/**
	 * if populated (not null) this object will be used by the putStreamObject method.
	 * @param inmd
	 */
	public void setMetaData(ObjectMetadata inmd) {
		metadata = inmd;
	}
}
