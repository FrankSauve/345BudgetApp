package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.budgetapp.resource.ResourceIT;

public class ShadowReader {

	private Connection conPostgres;
	private Connection conMySQL;
	MySQLStorage mySQLStorage;
	PostgresStorage postgresStorage;

	private static final Logger LOGGER = LoggerFactory.getLogger(ShadowWriter.class);

	ShadowReader(Connection conPostgres, Connection conMySQL){
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

	public long shadowReadUser(){
		
		long inconsistencies = 0;
		
		//The user data
		String username = ResourceIT.randomEmail();
		String name = ResourceIT.randomAlphabets();
		String password = ResourceIT.randomAlphabets();
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		String currency = "";

		try {
			//Only insert user in postgres database for test purposes
			postgresStorage.insertUsers(username, password, name, timeStamp, currency);	
			
			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			checker.checkUsers();
			
			inconsistencies = checker.getNumInconsistencies();
			
			checker.close();
		}
		catch(Exception e) {
			System.out.println("CAUGHT");
		}
		
		return inconsistencies;
	}
	
}
