package com.cts.retail.jdbc;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.cts.retail.properties.PropertiesSetUp;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetMetaData;

/*TODO Auto-generated constructor stub
	THIS CLASS WILL DUMP TABLE DATA TO FILE DATA TO FILE USING SQL DRIVER ONLY.
	CONFIGURE THE DATABASE TYPE INTO CONFIG.PROPERTIES FILE UNDER DB CONFIG
	SET THE DUMP DIRECTORY INTO PROPERTY FILE UNDER DUMP SECTION
	PASS DATA DUMP TABLE NAME AND FILE NAME AS INPUT.
	
	class implements two methods
	dbTableToFileUsingJAVA
	dbTableToFileUsingSQL - prefer this
	 
*/
public class DumpDataToFile {
	
	

	public DumpDataToFile() {
		// TODO Auto-generated constructor stub
	}
	
	public boolean  dbTableToFileUsingSQL(String propFileName, String fileName, String dbTableName) throws SQLException {
	
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		
		// TODO Auto-generated method stub
		PropertiesSetUp prop1= new PropertiesSetUp();
		Properties prop = prop1.loadPropertiesFile("config.properties");
		DBConnectUtils utils = new DBConnectUtils(prop.getProperty("DB_TYPE"));
		
		System.out.println("Loading Properties");
		utils=utils.loadConnectionProperties("config.properties");
		
		System.out.println("Displaying loaded Properties");
		utils.displayConnectionProperties();
		
		con= (Connection) utils.dbEstablishConnection();

		try {
			
			stmt = con.createStatement();
			rs = stmt.executeQuery("select * INTO OUTFILE '"  + fileName + "' FIELDS TERMINATED BY '|' " + " from " + dbTableName);
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
				e1.printStackTrace();
				return false;
		} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return false;
		} finally{
			       rs.close();
			       stmt.close();
			       con.close();
		}
	    
		rs.close();
	    stmt.close();
	    con.close();
		
	    return true;
	}


	public boolean  dbTableToFileUsingJAVA(String propFileName, String fileName, String dbTableName) throws SQLException, IOException {
		
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
	
		
		// next three line load property file 
		PropertiesSetUp prop1= new PropertiesSetUp();
		Properties prop = prop1.loadPropertiesFile("config.properties");
		DBConnectUtils utils = new DBConnectUtils(prop.getProperty("DB_TYPE"));
		
		// set the file to dump data 
		PrintWriter out = new PrintWriter(new FileWriter(fileName));
		
		// Setting connection properties files
		System.out.println("seting connection object by passing Properties file for connection user and pwd");
		utils=utils.loadConnectionProperties("config.properties");
		
		System.out.println("Displaying loaded Properties");
		utils.displayConnectionProperties();
		
		//Establishing connection
		con= (Connection) utils.dbEstablishConnection();
		
		System.out.println("Connection established");
		
		try {
			
			stmt = con.createStatement();
			rs = stmt.executeQuery("select *  from " + dbTableName);
				
			ResultSetMetaData rsmetadata=  (ResultSetMetaData) rs.getMetaData();
						
			while(rs.next()){
				String str="";
				for (int i=1; i<=rsmetadata.getColumnCount(); i++){
					str= str.concat(rs.getString(i)).concat("|");
				}
				System.out.println();
				out.println(str);
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
				e1.printStackTrace();
				return false;
		} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return false;
		} finally{
		
				rs.close();
			    stmt.close();
			    con.close();
			   	out.close();
		}
	
		rs.close();
	    stmt.close();
	    con.close();
		out.close();
		
	    return true;
	}

	
	
	
	public static void main(String[] args) throws SQLException, IOException {
		// TODO Auto-generated method stub
		boolean b;
		DumpDataToFile a =  new DumpDataToFile();
		
 		//call if you like to dump with the help of database
		
		//b=a.dbTableToFileUsingSQL("config.properties", "c:\\/Harish\\/product_order_media.dat", "product.order_media");

		//call if you like to dump with the help of java
		
		b=a.dbTableToFileUsingJAVA("config.properties", "c:\\/Harish\\/product_order_media.dat", "product.order_media");
		System.out.print("status" + b);
	}

}
