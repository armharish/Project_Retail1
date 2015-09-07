package com.cts.retail.jdbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;

import com.cts.retail.properties.PropertiesSetUp;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

public class LoadDataCsvToTable {

	public LoadDataCsvToTable() {
		// TODO Auto-generated constructor stub
	}

	public boolean loadCSVFileToTableUsingSQL(String propFileName, String fileName, String tableName) throws SQLException{
		

		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;
		
		
		// TODO Auto-generated method stub
		PropertiesSetUp prop1= new PropertiesSetUp();
		Properties prop = prop1.loadPropertiesFile(propFileName);
		DBConnectUtils utils = new DBConnectUtils(prop.getProperty("DB_TYPE"));
		
		System.out.println("Loading Properties");
		utils=utils.loadConnectionProperties("config.properties");
		
		System.out.println("Displaying loaded Properties");
		utils.displayConnectionProperties();
		
		con= (Connection) utils.dbEstablishConnection();

		try {
			String loadQuery  = "LOAD DATA LOCAL INFILE " + " '"  + fileName + "' " + " INTO TABLE " + tableName + " FIELDS TERMINATED BY '|' " + " LINES TERMINATED BY '\n'  " + prop.getProperty(tableName);
			System.out.println(loadQuery);
			stmt = con.createStatement();
			rs = stmt.executeQuery(loadQuery);
			
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
	
public boolean loadCSVFileToTableUsingJava(String propFileName, String fileName, String tableName, String indelimitter) throws SQLException{
		

		Connection con = null;
		Statement stmt = null;
		PreparedStatement pstmt= null;
		ResultSet rs = null;
		
		
		// TODO Auto-generated method stub
		PropertiesSetUp prop1= new PropertiesSetUp();
		Properties prop = prop1.loadPropertiesFile(propFileName);
		DBConnectUtils utils = new DBConnectUtils(prop.getProperty("DB_TYPE"));
		
		System.out.println("Loading Properties");
		utils=utils.loadConnectionProperties("config.properties");
		
		System.out.println("Displaying loaded Properties");
		utils.displayConnectionProperties();
		
		con= (Connection) utils.dbEstablishConnection();

		try {
			String insertQuery  = " INSERT INTO TABLE " + tableName + prop.getProperty(tableName) + " VALUES " + prop.getProperty(tableName+"_VALUES");
			pstmt=(PreparedStatement) con.prepareStatement(insertQuery);
			System.out.println(insertQuery);
			
			FileReader fr = new FileReader(fileName);
			BufferedReader br = new BufferedReader(fr);
			String oneline;
			
			while ((oneline=br.readLine())!=null){
				StringTokenizer token = new StringTokenizer(oneline,indelimitter);
				System.out.println(oneline);
				
				for (int i=1;token.hasMoreElements();i++){
					pstmt.setString(i, (String) token.nextElement());
					System.out.println(i+ " token is  " + token.nextElement());
				}
				System.out.println(pstmt.toString());
				pstmt.execute();
			}
			
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
				e1.printStackTrace();
				return false;
		
		} finally{

			       con.close();
			       
		}
	    
		rs.close();
	    stmt.close();
	    con.close();
	    
		
	    return true;
	}
	
	
public boolean loadCSVFileToTableUsingCSVLoader(String propFileName, String fileName, String tableName) throws SQLException{
	

	Connection con = null;
	Statement stmt = null;
	PreparedStatement pstmt= null;
	ResultSet rs = null;
	
	
	// TODO Auto-generated method stub
	PropertiesSetUp prop1= new PropertiesSetUp();
	Properties prop = prop1.loadPropertiesFile(propFileName);
	DBConnectUtils utils = new DBConnectUtils(prop.getProperty("DB_TYPE"));
	
	System.out.println("Loading Properties");
	utils=utils.loadConnectionProperties("config.properties");
	
	System.out.println("Displaying loaded Properties");
	utils.displayConnectionProperties();
	
	con= (Connection) utils.dbEstablishConnection();

	
	//CSVLoader csvloader=new CSVloader();
	con.close();
	
    return true;
}
	
	public static void main(String[] args)  {
		// TODO Auto-generated method stub

		LoadDataCsvToTable abc= new LoadDataCsvToTable();
		
		try {
			Boolean b= abc.loadCSVFileToTableUsingJava("config.properties", "c:\\/Harish\\/product_order_media_load.dat", "PRODUCT.ORDER_MEDIA","|");
			if (b==false){
				System.out.println("load failed");
			}
			else{
				System.out.println("File loaded into database successfully");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		/*
		
		try {
			Boolean b= abc.loadCSVFileToTableUsingSQL("config.properties", "c:\\/Harish\\/product_order_media_load.dat", "PRODUCT.ORDER_MEDIA");
			
			if (b==false){
				System.out.println("load failed");
			}
			else{
				System.out.println("File loaded into database successfully");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}

}
