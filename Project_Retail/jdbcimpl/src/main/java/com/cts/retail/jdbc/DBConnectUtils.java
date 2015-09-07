package com.cts.retail.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.cts.retail.properties.PropertiesSetUp;

public class DBConnectUtils {

	private String dbURL;
	private String dbLoginId;
	private String dbLoginPwd;
	Connection con = null;
	
	public DBConnectUtils() {
		// TODO Auto-generated constructor stub
	
	}

	public DBConnectUtils(String driverType) {
		// TODO Auto-generated constructor stub
		try {
				switch (driverType){
				
				case  "mysql":
					System.out.println("inside switch to register DB conection");
					Class.forName("com.mysql.jdbc.Driver").newInstance();
					break;
					
				
				default: 
					System.out.println("Driver in input is not supported" + driverType);
					break;
				}
		
		
		} catch (InstantiationException | IllegalAccessException
				|  ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	public DBConnectUtils loadConnectionProperties(String PropertyFileName){
		
		PropertiesSetUp prop = new PropertiesSetUp();
				
		Properties prop1=prop.loadPropertiesFile(PropertyFileName);
			
		this.dbURL=prop1.getProperty("DB_URL");
		this.dbLoginId=prop1.getProperty("DB_LOGON_USER");
		this.dbLoginPwd=prop1.getProperty("DB_LOGON_PWD");
		
		return this;
		
	}
	
	public void displayConnectionProperties(){
				
			System.out.println(this.dbURL);
			System.out.println(this.dbLoginId);
			System.out.println(this.dbLoginPwd);
			
		}
	
	
	public Connection dbEstablishConnection(){
	
		try {
			 con =DriverManager.getConnection(this.dbURL, this.dbLoginId, this.dbLoginPwd);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Connection to database " + this.dbURL + "Failed! Check output console");
			e.printStackTrace();
		}
		
		return con;
	}
		
	public static void main(String[] args) throws SQLException {
		
		Statement stmt = null;
		ResultSet rs = null;
		
		DBConnectUtils utils = new DBConnectUtils("mysql");
		
		System.out.println("Inside Main: Prinitng Properties before loading");
		utils.displayConnectionProperties();
		System.out.println("Loading Properties");
		utils.loadConnectionProperties("config.properties");
		System.out.println("Inside Main: Prinitng Properties after loading");
		utils.displayConnectionProperties();
		Connection con=utils.dbEstablishConnection();
		stmt = con.createStatement();

		//rs = stmt.executeQuery("select * from product.order_media");
			try{
			rs = stmt.executeQuery("select requested_media_code,requested_media_name  " + " INTO OUTFILE  'c:\\/Harish\\/my_table_widgets5.out'" + " FIELDS TERMINATED BY '|' " +"  from product.order_media");
			}
			catch(Exception e){
			
			System.out.println(e.getMessage());
			}
			finally{
			       rs.close();
			       stmt.close();
			       con.close();
			}
		

	/*while(rs.next()){
	          //Retrieve by column name
	          
	          String mediaCode = rs.getString("requested_media_code");
	         
	          //Display values
	          System.out.print("Media Code: " + mediaCode);

	       } */
	       //STEP 6: Clean-up environment 
	       rs.close();
	       stmt.close();
	       con.close();
	}
	//*/

}
