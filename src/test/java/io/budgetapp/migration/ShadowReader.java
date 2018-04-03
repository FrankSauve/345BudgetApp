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
			//Only insert in postgres database for shadow read consistency checker
			postgresStorage.insertUsers(username, password, name, timeStamp, currency);	
			
			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			checker.checkUsers();
			
			inconsistencies = checker.getNumInconsistencies();
		}
		catch(Exception e) {
			System.out.println("CAUGHT");
		}
		
		return inconsistencies;
	}
	
	public long shadowReadBudgetType() {
		
		long inconsistencies = 0;

		// The Budget Type Data
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());

		if(mySQLStorage == null) {
			System.out.println("ERROR");
		}
		
		try {
			//Only insert in postgres database for shadow read consistency checker
			postgresStorage.insertBudgetTypes(timeStamp);

			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			checker.checkBudgetTypes();
			
			inconsistencies = checker.getNumInconsistencies();
			
			checker.resetConsistencyChecker(); 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return inconsistencies;
	}

	public long shadowReadBudget() {
		
		long inconsistencies = 0;

		// The Budget Data
		String name = ResourceIT.randomAlphabets();
		double projected = 34.0;
		double actual = 39.0;
		Date periodOn = new Date(2018, 03, 31);
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int userId = 2;
		int categoryId = 2; 
		int typeId = 1;

		try {
			//Only insert in postgres database for shadow read consistency checker
			postgresStorage.insertBudgets(name, projected, actual, periodOn, timeStamp, userId, categoryId, typeId);

			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			checker.checkBudgets();
			
			inconsistencies = checker.getNumInconsistencies();
			
			checker.getNumInconsistencies();
			
			checker.resetConsistencyChecker();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return inconsistencies;
	}

	public long shadowReadCategories() {
		
		long inconsistencies = 0;

		// The Categories Data
		int id = 2001;
		String name = ResourceIT.randomAlphabets(); 
		String type = ResourceIT.randomAlphabets();
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int userId = 20000;

		try {
			//Only insert in postgres database for shadow read consistency checker
			postgresStorage.insertCategories(id, name, type, timeStamp, userId);

			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			checker.checkCategories();
			
			inconsistencies = checker.getNumInconsistencies();
			
			checker.resetConsistencyChecker();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return inconsistencies;
	}

	public long shadowReadRecurrings() {
		
		long inconsistencies = 0;

		//The recurrings data 
		double amount = 34.0;
		String type = ResourceIT.randomAlphabets(); 
		Timestamp lastRun = new Timestamp(System.currentTimeMillis());
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int budgetTypeId = 10; 
		String remark = ResourceIT.randomAlphabets();

		try {
			//Only insert in postgres database for shadow read consistency checker
			postgresStorage.insertRecurrings(amount, type, lastRun, timeStamp, budgetTypeId, remark);

			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			checker.checkRecurrings();
			
			inconsistencies = checker.getNumInconsistencies();
			
			checker.resetConsistencyChecker();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return inconsistencies;
	}

	public long shadowReadTransactions() {
		
		long inconsistencies = 0;

		//The transaction data
		String name = ResourceIT.randomAlphabets();
		int amount = 24; 
		String remark = ResourceIT.randomAlphabets();
		boolean auto = true;
		Timestamp transactionOn = new Timestamp(System.currentTimeMillis());
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int budgetId = 24;
		int recurringId = 24;

		try {
			//Only insert in postgres database for shadow read consistency checker
			postgresStorage.insertTransactions(name, amount, remark, auto, transactionOn, timeStamp, budgetId, recurringId);

			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			checker.checkTransactions();
			
			inconsistencies = checker.getNumInconsistencies();
			
			checker.resetConsistencyChecker();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
		return inconsistencies;

	}
	
}
