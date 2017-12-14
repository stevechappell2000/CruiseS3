package com.cruise.plugins.CruiseS3;

import java.util.HashMap;
import java.util.Properties;

import com.amazonaws.services.s3.model.Region;
import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.ActionParameter;
import com.cruise.plugins.PlugInMetaData;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class CruiseS3Buckets {
	private static HashMap<String, CruiseS3Bucket> bucketCache = null;
	/*
    	pmd.getActions().get(3).getActionParams().add(new ActionParameter("ConnectionName","true","MyAWSConnection","This is the unique name that this connection is stored under"));
    	pmd.getActions().get(3).getActionParams().add(new ActionParameter("accessKey","true","shortvalue","this is the shorter of the access keys"));
    	pmd.getActions().get(3).getActionParams().add(new ActionParameter("secretKey","true","longervalue","this is the longer of the access keys"));
    	pmd.getActions().get(3).getActionParams().add(new ActionParameter("region","true",Region.US_West_2.toString(),"Enter the AWS Region"));
    	pmd.getActions().get(3).getActionParams().add(new ActionParameter("bucketName","true","Yourbucket","Name of the bucket to access")); 
	 */
	@JsonIgnore
	public static CruiseS3Bucket getBucket(Services s, PlugInMetaData localConfig) throws Exception {
		CruiseS3Bucket ret = null;
        if(null == bucketCache) {
        	bucketCache = new HashMap<String, CruiseS3Bucket>();
        }
        String connName = s.Parameter("connectionName");
        if(bucketCache.containsKey(connName)) {
        	ret = bucketCache.get(connName);
        }else {
        	String accessKey = null;
        	if(null == s.Parameter("accessKey") || s.Parameter("accessKey").length()<1) {
        		accessKey = localConfig.getConfiguration().getConfig().getProperty("accessKey");
        	}else {
        		accessKey = s.Parameter("accessKey");
        	}
        	String secretKey = null;
        	if(null == s.Parameter("secretKey") || s.Parameter("secretKey").length()<1) {
        		secretKey = localConfig.getConfiguration().getConfig().getProperty("secretKey");
        	}else {
        		secretKey = s.Parameter("secretKey");
        	}
        	String region = s.Parameter("region");
        	ret = new CruiseS3Bucket(accessKey, secretKey, region);
        	bucketCache.put(connName, ret);
        }
        return ret;
	}
	public Properties getConnectionList() {
		Properties list = new Properties();
		if(null != bucketCache) {
			
		}
		
		return list;
		
	}
}
