package com.cts.retail.properties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesSetUp {

	public PropertiesSetUp() {
		// TODO Auto-generated constructor stub
	}

	public Properties loadPropertiesFile(String PropertyFileName){
		
			Properties prop = new Properties();
			InputStream input = null;
			
			try {
				input = new FileInputStream(PropertyFileName);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				prop.load(input);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return prop;
	}
}
