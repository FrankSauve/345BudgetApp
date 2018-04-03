package io.budgetapp.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConnector {
	private static PostgresConnector singleton = new PostgresConnector( );
	private PostgresConnector() { }
	private String host = "jdbc:postgresql://localhost:5432/345BudgetApp";
	private String username = "postgres";
	private String password = "postgres";
   
	public static PostgresConnector getInstance( ) {
		return singleton;
	}
	
	public Connection getPostgresConnection() throws SQLException {		
		return DriverManager.getConnection(host, username, password);
	}
}
