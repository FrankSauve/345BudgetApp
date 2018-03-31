package io.budgetapp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.ResultSet;

public class MySQLStorage{
	
	String host1 = "jdbc:postgresql://localhost:5432/345BudgetApp";
	String username1 = "postgres";
	String password1 = "postgres";
	
	String host2 = "jdbc:mysql://localhost:3306/345BudgetApp";
	String username2 = "root";
	String password2 = "";
	

	public void forklift() {
		
		System.out.println("***********************\n**************************\n*****************");
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);
			
			// Getting values from the old database (Postgres)
			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM users");
			
			while(result.next()) {
				System.out.println("There is something in the result set");
				int id = result.getInt("id");
				System.out.println("id: " + id);
				String username= result.getString("username");
				System.out.println("username: " + username);
				String password = result.getString("password");
				System.out.println("password: " + password);
				String name = result.getString("name");
				System.out.println("name: " + name);
				Timestamp timeStamp = result.getTimestamp("created_at");
				System.out.println("created_at" + timeStamp );
				String currency = result.getString("currency");
				System.out.println("currency:" + currency);
				
				
				
			      String query = " insert into users (username, password, name, created_at, currency)"
			    	        + " values (?, ?, ?, ?, ?)";
			      PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
			      preparedStmt.setString(1, username);
			      preparedStmt.setString(2, password);
			      preparedStmt.setString(3, name);
			      preparedStmt.setTimestamp(4, timeStamp);
			      preparedStmt.setString(5, currency);
				
			      preparedStmt.execute();
			      
			}
			
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
			
		}
		System.out.println("***********************\n**************************\n*****************");
		
	}
	
}