package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;


public class MigrationTest{

	private static final Logger LOGGER = LoggerFactory.getLogger(MigrationTest.class);

	String host1 = "jdbc:postgresql://localhost:5432/345BudgetApp";
	String username1 = "postgres";
	String password1 = "postgres";

	String host2 = "jdbc:mysql://localhost:3306/345BudgetApp";
	String username2 = "root";
	String password2 = "root";
	
	MySQLStorage mySQLStorage;

	/**
	 * Migrates the user table
	 */
	public void forkliftUsers() {
		LOGGER.info("**************Forklift Users***************");
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM users");

			while(result.next()) {

				// Getting values from the old database (Postgres)
				int id = result.getInt("id");
				LOGGER.debug("id: " + id);

				String username= result.getString("username");
				LOGGER.debug("username: " + username);
				String password = result.getString("password");
				LOGGER.debug("password: " + password);
				String name = result.getString("name");
				LOGGER.debug("name: " + name);
				Timestamp timeStamp = result.getTimestamp("created_at");
				LOGGER.debug("created_at" + timeStamp );
				String currency = result.getString("currency");
				LOGGER.debug("currency:" + currency);

				// Copying data into new storage (MySQL)
				mySQLStorage = new MySQLStorage(conMySQL);
				mySQLStorage.insertUsers(username, password, name, timeStamp, currency);


			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Migrate the budget_types data
	 */
	public void forkliftBudgetTypes() {
		LOGGER.info("**************Forklift budget_types***************");
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM budget_types");

			while(result.next()) {

				// Getting values from the old database (Postgres)
				int id = result.getInt("id");
				LOGGER.debug("id: " + id);

				Timestamp timeStamp = result.getTimestamp("created_at");
				LOGGER.debug("created_at" + timeStamp );

				// Copying data into budget table
				mySQLStorage = new MySQLStorage(conMySQL);
				mySQLStorage.insertBudgetTypes(timeStamp);

			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Migrate the Budgets table
	 */
	public void forkliftBudgets() {
		LOGGER.info("**************Forklift budget***************");
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM budgets");

			while(result.next()) {

				// Getting values from the old database (Postgres)
				int id = result.getInt("id");
				LOGGER.debug("id: " + id);

				String name = result.getString("name");
				LOGGER.debug("name: " + name);
				double projected = result.getDouble("projected");
				LOGGER.debug("projected: " + projected);
				double actual = result.getDouble("actual");
				LOGGER.debug("actual: " + actual);
				Date periodOn= result.getDate("period_on");
				LOGGER.debug("period_on" + periodOn );
				Timestamp timeStamp = result.getTimestamp("created_at");
				LOGGER.debug("created_at" + timeStamp );
				int userId = result.getInt("user_id");
				LOGGER.debug("user_id" + userId);
				int categoryId = result.getInt("category_id");
				LOGGER.debug("category_id" + categoryId);
				int typeId = result.getInt("type_id");
				LOGGER.debug("type_id" + typeId);

				// Copying data into budget table
				mySQLStorage = new MySQLStorage(conMySQL);
				mySQLStorage.insertBudgets(name, projected, actual, periodOn, timeStamp, userId, categoryId, typeId);
			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Migrate the categories table
	 */
	public void forkliftCategories() {
		LOGGER.info("**************Forklift categories***************");
		
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM categories");

			while(result.next()) {

				// Getting values from the old database (Postgres)
				int id = result.getInt("id");
				LOGGER.debug("id: " + id);

				String name = result.getString("name");
				LOGGER.debug("name: " + name);
				String type = result.getString("type");
				LOGGER.debug("type: " + type);
				Timestamp timeStamp = result.getTimestamp("created_at");
				LOGGER.debug("created_at" + timeStamp );
				int userId = result.getInt("user_id");
				LOGGER.debug("user_id" + userId);

				// Copying data into budget table
				mySQLStorage = new MySQLStorage(conMySQL);
				mySQLStorage.insertCategories(id, name, type, timeStamp, userId);
			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Migrate the recurrings tables
	 */
	public void forkliftRecurrings(){
		LOGGER.info("**************Forklift recurrings***************");
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM recurrings");

			while(result.next()) {

				// Getting values from the old database (Postgres)
				int id = result.getInt("id");
				LOGGER.debug("id: " + id);

				double amount = result.getDouble("amount");
				LOGGER.debug("amount: " + amount);
				String type = result.getString("type");
				LOGGER.debug("type: " + type);
				Timestamp lastRun = result.getTimestamp("last_run_at");
				LOGGER.debug("last_run_at" + lastRun );
				Timestamp timeStamp = result.getTimestamp("created_at");
				LOGGER.debug("created_at" + timeStamp );
				int budgetTypeId = result.getInt("budget_type_id");
				LOGGER.debug("budget_type_id" + budgetTypeId);
				String remark = result.getString("remark");
				LOGGER.debug("remark:" + remark);

				// Copying data into budget table
				mySQLStorage = new MySQLStorage(conMySQL);
				mySQLStorage.insertRecurrings(amount, type, lastRun, timeStamp, budgetTypeId, remark);
			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Migrate the transactions table
	 */
	public void forkliftTransactions() {
		LOGGER.info("**************Forklift transactions***************");
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM transactions");

			while(result.next()) {

				// Getting values from the old database (Postgres)
				int id = result.getInt("id");
				LOGGER.debug("id: " + id);

				String name = result.getString("name");
				LOGGER.debug("name: " + name);
				double amount = result.getDouble("amount");
				LOGGER.debug("amount: " + amount);
				String remark = result.getString("remark");
				LOGGER.debug("remark:" + remark);
				boolean auto = result.getBoolean("auto");
				LOGGER.debug("auto:" + auto);
				Timestamp transactionOn = result.getTimestamp("transaction_on");
				LOGGER.debug("transaction_on" + transactionOn );
				Timestamp timeStamp = result.getTimestamp("created_at");
				LOGGER.debug("created_at" + timeStamp );
				int budgetId = result.getInt("budget_id");
				LOGGER.debug("budget_id:" + budgetId);
				int recurringId = result.getInt("recurring_id");
				LOGGER.debug("recurring_id:" + recurringId);

				// Copying data into budget table
				mySQLStorage = new MySQLStorage(conMySQL);
				mySQLStorage.insertTransactions(name, amount, remark, auto, transactionOn, timeStamp, budgetId, recurringId);
			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Migrate data from postgres to mysql
	 */
	public void forklift() {
		LOGGER.info("**************Forklift Active***************");
		LOGGER.info("**************Tranferring Data***************");
				forkliftUsers();
				forkliftBudgetTypes();
				forkliftBudgets();
				forkliftCategories();
				forkliftRecurrings();
				forkliftTransactions();
		LOGGER.info("**************Forklift Deactive***************");
		LOGGER.info("**************Transfer Stopped****************");
	}


	/**
	 * Compares all of the forklifted data and corrects any inconsistencies.
	 */
	public void checkAll(){
		LOGGER.info("*********Activating Consistency Checker***********");
		
		try {
			Connection conPostgres = DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);
			
			ConsistencyChecker checker = new ConsistencyChecker(conPostgres, conMySQL);
			
			checker.checkUsers();
			checker.checkBudgetTypes();
			checker.checkBudgets();
			checker.checkCategories();
			checker.checkRecurrings();
			checker.checkTransactions();
			
			checker.close();
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		LOGGER.info("*********Deactivating Consistency Checker***********");
	}

	@Test
	public void migrationTest() {

		// forklift the data from old storage to new 
		forklift();

		// check for inconsistencies 
		// ensure inconsistencies are fixed 
		checkAll();


		//Start consistency checker asynchronously
		new Thread( new Runnable() {
			public void run(){
				checkAll();
				return; 
			}
	    }).start();
		
		
		//shadow writes 

		// shadow reads for validation 


	}

}