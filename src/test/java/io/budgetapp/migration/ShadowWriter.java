package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShadowWriter {

	private Connection conPostgres;
	private Connection conMySQL;
	MySQLStorage mySQLStorage;
	PostgresStorage postgresStorage;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ShadowWriter.class);

	ShadowWriter(Connection conPostgres, Connection conMySQL){
		this.conPostgres = conPostgres;
		this.conMySQL = conMySQL;
	}

	public void close() throws SQLException {
		conMySQL.close();
		conPostgres.close();
	}
	
	public void connectToDatabases() {
		mySQLStorage = new MySQLStorage(conMySQL);
		postgresStorage = new PostgresStorage(conPostgres);
	}

	public void shadowWriteUser(){
		
		//The user data
		String username = "john.doe@gmail.com";
		String name = "John Doe";
		String password = "5f4dcc3b5aa765d61d8327deb882cf99";
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		String currency = "";

		try {
			// Shadow writing to users table
			mySQLStorage.insertUsers(username, password, name, timeStamp, currency);
			postgresStorage.insertUsers(username, password, name, timeStamp, currency);	
		}
		catch(Exception e) {
			System.out.println("CAUGHT");
		}
		
		ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
		checker.checkUsers();
	}
	


}


