package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.budgetapp.resource.ResourceIT;
import junit.framework.Assert;

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

	public void shadowReadUser(){
		
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
			Assert.assertTrue(checker.getNumInconsistencies() > 0); //There should be inconsistencies
			
			checker.resetConsistencyChecker();
			
			checker.checkUsers();
			Assert.assertEquals(0, checker.getNumInconsistencies()); //inconsistencies should be fixed now
			
			checker.close();
		}
		catch(Exception e) {
			System.out.println("CAUGHT");
		}
	}
	
	public void shadowReadBudgetType() {

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
			Assert.assertTrue(checker.getNumInconsistencies() > 0); //There should be inconsistencies
			
			checker.resetConsistencyChecker();
			
			checker.checkBudgetTypes();
			Assert.assertEquals(0, checker.getNumInconsistencies()); //inconsistencies should be fixed now 
			
			checker.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void shadowReadBudget() {

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
			Assert.assertTrue(checker.getNumInconsistencies() > 0); //There should be inconsistencies
			
			checker.resetConsistencyChecker();
			
			checker.checkBudgets();
			Assert.assertEquals(0, checker.getNumInconsistencies()); //inconsistencies should be fixed now
			
			checker.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void shadowReadCategories() {

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
			Assert.assertTrue(checker.getNumInconsistencies() > 0); //There should be inconsistencies
			
			checker.resetConsistencyChecker();
			
			checker.checkCategories();
			Assert.assertEquals(0, checker.getNumInconsistencies()); //inconsistencies should be fixed now
			
			checker.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void shadowReadRecurrings() {

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
			Assert.assertTrue(checker.getNumInconsistencies() > 0); //There should be inconsistencies
			
			checker.resetConsistencyChecker();
			
			checker.checkRecurrings();
			Assert.assertEquals(0, checker.getNumInconsistencies()); //inconsistencies should be fixed now
			
			checker.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public void shadowReadTransactions() {
		
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
			Assert.assertTrue(checker.getNumInconsistencies() > 0); //There should be inconsistencies
			
			checker.resetConsistencyChecker();
			
			checker.checkTransactions();
			Assert.assertEquals(0, checker.getNumInconsistencies()); //inconsistencies should be fixed now
			
			checker.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}

	}
	
}
