package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
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

	public void shadowWriteBudgetType() {

		// The Budget Type Data
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());

		if(mySQLStorage == null) {
			System.out.println("ERROR");
		}

		// Shadow writing to budget-types table
		mySQLStorage.insertBudgetTypes(timeStamp);
		postgresStorage.insertBudgetTypes(timeStamp);

		ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
		checker.checkUsers();
	}

	public void shadowWriteBudget() {

		// The Budget Data
		String name = "Wages & Tips";
		double projected = 34.0;
		double actual = 39.0;
		Date periodOn = new Date(2018, 03, 31);
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int userId = 2;
		int categoryId = 2; 
		int typeId = 1;

		// Shadow writing to budget-types table
		mySQLStorage.insertBudgets(name, projected, actual, periodOn, timeStamp, userId, categoryId, typeId);
		postgresStorage.insertBudgets(name, projected, actual, periodOn, timeStamp, userId, categoryId, typeId);

		ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
		checker.checkUsers();
	}

	public void shadowWriteCategories() {

		// The Categories Data
		int id = 2001;
		String name = "Testing"; 
		String type = "INCOME";
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int userId = 20000;

		// Shadow writing to categories table
		mySQLStorage.insertCategories(id, name, type, timeStamp, userId);
		postgresStorage.insertCategories(id, name, type, timeStamp, userId);

		ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
		checker.checkUsers();
	}

	public void shadowWriteRecurrings() {

		//The recurrings data 
		double amount = 34.0;
		String type = "TESTING"; 
		Timestamp lastRun = new Timestamp(System.currentTimeMillis());
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int budgetTypeId = 10; 
		String remark = "TESTING";

		// Shadow writing to recurrings table
		mySQLStorage.insertRecurrings(amount, type, lastRun, timeStamp, budgetTypeId, remark);
		postgresStorage.insertRecurrings(amount, type, lastRun, timeStamp, budgetTypeId, remark);

		ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
		checker.checkUsers();

	}

	public void shadowWriteTransactions() {

		//The transaction data
		String name = "Test";
		int amount = 24; 
		String remark = "TESTING";
		boolean auto = true;
		Timestamp transactionOn = new Timestamp(System.currentTimeMillis());
		Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
		int budgetId = 24;
		int recurringId = 24;

		//Shadow writing to transactions table 
		mySQLStorage.insertTransactions(name, amount, remark, auto, transactionOn, timeStamp, budgetId, recurringId);
		postgresStorage.insertTransactions(name, amount, remark, auto, transactionOn, timeStamp, budgetId, recurringId);

		ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
		checker.checkUsers();

	}

}