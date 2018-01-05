package com.cruise.plugins.CruiseS3;

import java.util.Properties;

import com.corecruise.cruise.services.interfaces.CruiseConfigurationInterface;



public class CruiseS3Config implements CruiseConfigurationInterface{
    private Properties props = null;

	public boolean initConfig() {
		if(null == props) {
			props = new Properties();
			props.put("accessKey", "AKIAIRS4VXIUU6P3QT4Q");
			props.put("secretKey", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		}
		return true;
	}

	public Properties getConfig() {
		// TODO Auto-generated method stub
		return props;
	}

}
