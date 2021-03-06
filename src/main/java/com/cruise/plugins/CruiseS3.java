package com.cruise.plugins;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Region;
import com.corecruise.core.CoreCruise;
import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.config.CruisePluginEnvironment;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.Application;
import com.corecruise.cruise.services.utils.GenericSessionResp;
import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.Action;
import com.cruise.plugins.ActionParameter;
import com.cruise.plugins.PlugInMetaData;
import com.cruise.plugins.utils.CruiseS3Bucket;
import com.cruise.plugins.utils.CruiseS3Buckets;


public class CruiseS3 implements PluginInterface
{


	PlugInMetaData pmd = null;
	CruisePluginEnvironment config = null;
	String pluginName = "CruiseS3";
	HashMap<String,String> cachedService = new HashMap<String,String>();
	public CruiseS3() {
		if(null == config)
			config = CoreCruise.getCruiseConfig(pluginName);
		
    	pmd = new PlugInMetaData(pluginName,"0.0.1","SJC","AWS S3 Storage");
    	
    	int x=0;
    	pmd.getActions().add(new Action("info", "getPlugin Information"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	
    	++x;
    	pmd.getActions().add(new Action("PlugInInfo", "get information about the pluging"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	
    	++x;
    	pmd.getActions().add(new Action("echo", "Echos the request back as response."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	
    	++x;
    	pmd.getActions().add(new Action("s3Connect", "Creates a connection to an S3 Bucket. All other calls only need to refer to the connectionName."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","This is the unique name that this connection is stored under"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("accessKey","false","","this is the shorter of the access keys"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("secretKey","false","","this is the longer of the access keys"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("region","true",Region.US_West_2.toString(),"Enter the AWS Region"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","Yourbucket","Name of the bucket to access"));
    	
    	++x;
    	pmd.getActions().add(new Action("s3ListBuckets", "Lists all the S3Buckets for the current connection"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name."));  
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","Yourbucket","Name of the bucket to access"));
    	
    	++x;
    	pmd.getActions().add(new Action("attachService", "Gets a String object from an S3 Bucket."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("attach","true","~UUID","Name of service to attach, or a comma seperated list of services to attach."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","MyAWSConnection","The valid bucketname for this connection.")); 
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("objectName","true","Yourbucket","This is the actual file/object name to retrieve."));    	
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("cache","false","false","When true, the S3 Object will be cached in local memory."));   

    	++x;
    	pmd.getActions().add(new Action("resetCache", "Lists all the files in the current bucket"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));

    	
    	++x;
    	pmd.getActions().add(new Action("s3ListAllFiles", "Lists all the files in the current bucket"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name.")); 
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","MyAWSConnection","The valid bucketname for this connection.")); 
    	
    	++x;
    	pmd.getActions().add(new Action("s3ListPrefixFiles", "Lists all the files in the current bucket"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name.")); 
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","MyAWSConnection","The valid bucketname for this connection.")); 
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("prefix","true","MyAWSConnection","Limit selection to files/objects with this prefix.")); 
    	
    	++x;
    	pmd.getActions().add(new Action("s3PutString", "Puts an String object into an S3 Bucket."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","MyAWSConnection","The valid bucketname for this connection.")); 
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("objectName","true","Yourbucket","This is the actual file/object name to save the object as."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("object","true","Yourbucket","String to save into the object."));
    	
    	++x;
    	pmd.getActions().add(new Action("s3GetString", "Gets a String object from an S3 Bucket."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","MyAWSConnection","The valid bucketname for this connection.")); 
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("objectName","true","Yourbucket","This is the actual file/object name to retrieve."));
        
    	++x;
    	pmd.getActions().add(new Action("s3Put", "Puts an object into an S3 Bucket."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("object","true","Yourbucket","This is normally associated with an object sent by something like a multipart form. This field is just the name"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","Yourbucket","Name of the bucket to access"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("objectName","true","Yourbucket","This is the actual file/object name to store."));
    	
    	++x;
    	pmd.getActions().add(new Action("s3Get", "Gets an object into an S3 Bucket."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","Yourbucket","Name of the bucket to access"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("objectName","true","Yourbucket","This is the actual file/object name to retrieve."));

    	++x;
    	pmd.getActions().add(new Action("s3DeleteObject", "Deletes an object from an S3 Bucket."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("connectionName","true","MyAWSConnection","The connection must already have been created using this name."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("bucketName","true","Yourbucket","Name of the bucket to access"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("objectName","true","Yourbucket","This is the actual file/object name to be removed."));

    
    
    }
	@Override
	public PlugInMetaData getPlugInMetaData() {
		// TODO Auto-generated method stub
		return pmd;
	}

	@Override
	public void setPluginVendor(PlugInMetaData PMD) {
		pmd = PMD;
		
	}
	public boolean executePlugin(SessionObject so, Services service) {
		boolean ret = false;
		String action = service.Action().trim().toLowerCase();
		GenericSessionResp gro = new GenericSessionResp();
		switch (action) {
		case "cruisetest":
			gro.addParmeter("PluginEnabled", "true");
			so.appendToResponse(service.Service()+"."+service.Action(),gro);
			ret = true;
			break;
		case "plugininfo":
			if(null != pmd) {
				so.appendToResponse("PlugInInfo", pmd);
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "600005", "Failed to get any information about the plugin.");
			}
			break;
		case "info":
			if(null != pmd) {
				so.appendToResponse("PlugInInfo", pmd);
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "600005", "Failed to get any information about the plugin.");
			}
			break;
		case "echo":
			if(null != pmd) {
				so.appendToResponse(so.getApplication());
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "600006", "Failed to get any information about the plugin.");
			}
			break;
		case "s3connect":
			//added endpoint
            try {
            	CruiseS3Bucket bucket = CruiseS3Buckets.getBucket(service, pmd, config);
				if(null != bucket) {
					gro.addParmeter("AWSConnection", "true");
					so.appendToResponse(service.Service()+"."+service.Action(),gro);
					ret = true;
				}
			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600007", "Failed to connect:"+e.getMessage());
				e.printStackTrace();
			}
			break;
		case "s3listbuckets":
            try {
            	CruiseS3Bucket bucket = CruiseS3Buckets.getBucket(service, pmd, config);
				if(null != bucket) {
					List<Bucket> myBuckets = bucket.listBuckets();
					so.appendToResponse(service.Service()+"."+service.Action(),myBuckets);
					ret = true;
				}
			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600008", "Failed to connect."+e.getMessage());
				e.printStackTrace();
			}
			break;
		case "s3listallfiles":
            try {
            	CruiseS3Bucket bucket = CruiseS3Buckets.getBucket(service, pmd, config);
				if(null != bucket) {
					ObjectListing myBuckets = bucket.listAllObjects(service.Parameter("bucketName"));
					so.appendToResponse(service.Service()+"."+service.Action(),myBuckets);
					ret = true;
				}
			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600009", "Failed to connect."+e.getMessage());
				e.printStackTrace();
			}
			break;
		case "s3listprefixfiles":
            try {
            	CruiseS3Bucket bucket = CruiseS3Buckets.getBucket(service, pmd, config);
				if(null != bucket) {
					ObjectListing myBuckets = bucket.listPrefixedObjects(service.Parameter("bucketName"),service.Parameter("prefix"));
					so.appendToResponse(service.Service()+"."+service.Action(),myBuckets);
					ret = true;
				}
			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600010", "Failed to connect."+e.getMessage());
				e.printStackTrace();
			}
			break;
		case "s3putstring":
            try {
            	CruiseS3Bucket bucket = CruiseS3Buckets.getBucket(service, pmd, config);
				if(null != bucket) {
					if(bucket.putObjectString(service.Parameter("bucketName"),service.Parameter("objectName"),service.Parameter("object"))) {
						gro.addParmeter("S3Put", "true");
						so.appendToResponse(service.Service()+"."+service.Action(), gro);
						ret = true;
					}else {
						gro.addParmeter("S3Put", "false");
						so.appendToResponse(service.Service()+"."+service.Action(), gro);
					}
				}
			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600011", "Failed to connect."+e.getMessage());
				e.printStackTrace();
			}
			break;
		case "s3getstring":
            try {
            	CruiseS3Bucket bucket = CruiseS3Buckets.getBucket(service, pmd, config);
				if(null != bucket) {
					String s3String = bucket.getObjectString(service.Parameter("bucketName"),service.Parameter("objectName"));
					if(null != s3String) {
						gro.addParmeter("S3get", "File Found.");
						gro.addParmeter("length", ""+s3String.length());
						gro.addParmeter("object", s3String);
						so.appendToResponse(service.Service()+"."+service.Action(), gro);
						ret = true;
					}else {
						gro.addParmeter("S3get", "No file, or empty file.");
						gro.addParmeter("object", "");
						so.appendToResponse(service.Service()+"."+service.Action(), gro);
					}
				}
			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600012", "Failed to connect."+e.getMessage());
				e.printStackTrace();
			}
			break;
		case "s3deleteobject":
            try {
            	CruiseS3Bucket bucket = CruiseS3Buckets.getBucket(service, pmd, config);
				if(null != bucket) {
					if(bucket.deleteObject(service.Parameter("bucketName"),service.Parameter("objectName"))){
						gro.addParmeter("s3DeleteObject", "Object Removed");
						so.appendToResponse(service.Service()+"."+service.Action(), gro);
					}else {
						gro.addParmeter("s3DeleteObject", "Failed");
						so.appendToResponse(service.Service()+"."+service.Action(), gro);
					}

				}
			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600014", "Failed to connect."+e.getMessage());
				e.printStackTrace();
			}
			break;
		case "s3get":

			break;
		case "attachservice":
			/**
			 * Key to supporting this function is to first get whatever connection you need, then get the raw JSON and convert it to an Address object
			 * You may have to append {"Application":{" to the JSON to get it to parse correctly.
			 * 
			 * You will then need to gather the pass-thru parameters simply add the parameter with the attached service name as the prefix.
			 * 
			 * for example;
			 * "attach" : "MyService",
			 * "MyService.value" : "some value to pass",
			 * "MyService.thisValue" : "some value to pass",
			 * "MyService.anotherValue3" : "some value to pass",
			 * 
			 * 
			 */
			try {
				String attach = service.Parameter("attach");
				String cache = service.Parameter("cache");
				if(null == cache) {
					cache = "false";
				}

				ArrayList<String> attachList = new ArrayList<String>();

				if(attach.contains(",")) {
					String s[] = attach.split(",");
					for(String sx:s) {
						attachList.add(sx.trim());
					}
				}else {
					attachList.add(attach.trim());
				}
				boolean ok = false;
				ArrayList<String> extParams = service.getExtraParameters(pmd);
				if(null != extParams && extParams.size()>0) {
					ok = true;
				}
				CruiseS3Bucket bucket = null;
				String s3String = null;
				boolean foundit = false;
				for(String objecName:attachList) {
					ret = true;
					foundit = false;
					if(cache.equalsIgnoreCase("true")) {
						if(cachedService.containsKey(objecName)) {
							foundit = true;
							s3String = cachedService.get(objecName);
							if(null != s3String) {
								//Create the Application Object
								Application a = so.getCruiseMapper().readValue("{\"Application\" : "+s3String+"}", Application.class);
								int x = service.getServiceIndex();
								// add Passthrough key value pairs
								for(Services s:a.getServices()) {
									++x;
									if(ok) {
										for(String ep:extParams) {
											if(null != s.getServiceName() && ep.startsWith(s.getServiceName())) {
												s.Parameter(ep.replace(s.getServiceName()+".", ""),service.Parameter(ep));
											}
										}
									}
									//add the news service to the list to be executed.
									so.getApplication().getServices().add(x, s);
								}
							}else {
								ret = false;
							}
						}
					}
					if(!foundit) {
						try {
							if(null == bucket) {
								bucket = CruiseS3Buckets.getBucket(service, pmd, config);
							}
							s3String = bucket.getObjectString(service.Parameter("bucketName"),objecName);
							if(null != s3String) {
								if(cache.equalsIgnoreCase("true")) {
									cachedService.put(objecName, s3String);
								}
								Application a = so.getCruiseMapper().readValue("{\"Application\" : "+s3String+"}", Application.class);
								int x = service.getServiceIndex();
								// add Passthrough key value pairs
								for(Services s:a.getServices()) {
									++x;
									if(ok) {
										for(String ep:extParams) {
											if(null != s.getServiceName() && ep.startsWith(s.getServiceName())) {
												s.Parameter(ep.replace(s.getServiceName()+".", ""),service.Parameter(ep));
											}
										}
									}
									//add the news service to the list to be executed.
									so.getApplication().getServices().add(x, s);
								}
							}else {
								Clog.Error(so, "PlugInInfo", "600015", "CruiseS3 Attach of script "+objecName+" was empty or not found.");
								ret = false;
							}

						}catch(Exception e) {
							Clog.Error(so, "PlugInInfo", "600013", "CruiseS3 Attach of script "+objecName+" was empty or not found."+e.getMessage());
							ret = false;
						}
					}

				}
				///

			} catch (Exception e) {
				Clog.Error(so, "PlugInInfo", "600022", "Failed to connect:"+e.getMessage());
				e.printStackTrace();
			}
			break;	
		default:
			Clog.Error(so, "service", "600000", "Invalid Action supplied:"+action);
		}
		return ret;

	}
	@Override
	public void byPass(SessionObject sessionObject) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public boolean initPlugin() {
		// TODO Auto-generated method stub
		return false;
	}


}
