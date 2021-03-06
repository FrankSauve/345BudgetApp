package io.budgetapp.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySqlConnector {
	private static MySqlConnector singleton = new MySqlConnector( );
	private MySqlConnector() { }
	private String host = "jdbc:mysql://localhost:3306/345BudgetApp";
	private String username = "root";
	private String password = "root";
	private boolean useMySql = false; //Toggle to use MySql
   
	public static MySqlConnector getInstance( ) {
		return singleton;
	}
	
	public Connection getMySqlConnection() throws SQLException {		
		return DriverManager.getConnection(host, username, password);
	}

	public boolean isUseMySql() {
		return useMySql;
	}

	public void setUseMySql(boolean useMySql) {
		this.useMySql = useMySql;
	}
	
}
