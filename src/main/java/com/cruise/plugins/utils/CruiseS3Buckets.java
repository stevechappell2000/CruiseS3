package com.cruise.plugins.utils;

import java.util.HashMap;
import java.util.Properties;
import com.corecruise.core.CoreCruise;
import com.corecruise.cruise.config.CruisePluginEnvironment;
import com.corecruise.cruise.services.utils.Services;
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
	public static CruiseS3Bucket getBucket(Services s, PlugInMetaData localConfig,CruisePluginEnvironment config) throws Exception {
		
		CruiseS3Bucket ret = null;
		try {
		if(null == config) {
			config = CoreCruise.getCruiseConfig("CruiseS3");
		}
        if(null == bucketCache) {
        	bucketCache = new HashMap<String, CruiseS3Bucket>();
        }
        String connName = s.Parameter("connectionName");
        if(bucketCache.containsKey(connName)) {
        	ret = bucketCache.get(connName);
        }else {
        	String accessKey = null;
        	if(null == s.Parameter("accessKey") || s.Parameter("accessKey").length()<1) {
        		accessKey = config.getPluginProperties().getProperty("accessKey");
        	}else {
        		accessKey = s.Parameter("accessKey");
        	}
        	String secretKey = null;
        	if(null == s.Parameter("secretKey") || s.Parameter("secretKey").length()<1) {
        		secretKey = config.getPluginProperties().getProperty("secretKey");
        	}else {
        		secretKey = s.Parameter("secretKey");
        	}
        	String endPoint = null;
        	if(null == s.Parameter("endPoint") || s.Parameter("endPoint").length()<1) {
        		endPoint = config.getPluginProperties().getProperty("endPoint");
        	}else {
        		endPoint = s.Parameter("endPoint");
        	}
        	String region = s.Parameter("region");
        	System.out.println("Accessing region:"+region);
        	//added Endpoint
        	ret = new CruiseS3Bucket(accessKey, secretKey, region, endPoint);
        	bucketCache.put(connName, ret);
        }
		}catch(Exception e) {
			e.printStackTrace();
		    //throw e;
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
