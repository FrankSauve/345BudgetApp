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

	/**
	 * Migrates the user table
	 */
	public void forkliftUsers() {

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
				String query = " INSERT INTO users (username, password, name, created_at, currency)"
						+ " VALUES (?, ?, ?, ?, ?)";
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				preparedStmt.setString(1, username);
				preparedStmt.setString(2, password);
				preparedStmt.setString(3, name);
				preparedStmt.setTimestamp(4, timeStamp);
				preparedStmt.setString(5, currency);


				try {
					preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("username " + username + " already exists");
				}
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
				
				// Copying data into new storage (MySQL)
				String query = " INSERT INTO budget_types (created_at) VALUES (?)";
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				preparedStmt.setTimestamp(1, timeStamp);
				
				try {
					preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("budget_types table migration failed");
					e.printStackTrace();
				}
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
				
				//Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

				// Copying data into new storage (MySQL)
				String query = " INSERT INTO budgets (name, projected, actual, period_on, created_at, user_id, category_id, type_id)"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				preparedStmt.setString(1, name);
				preparedStmt.setDouble(2, projected);
				preparedStmt.setDouble(3, actual);
				preparedStmt.setDate(4, periodOn);
				preparedStmt.setTimestamp(5, timeStamp);
				preparedStmt.setInt(6, userId);
				preparedStmt.setInt(7, categoryId);
				preparedStmt.setInt(8, typeId);

				try {
					preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("budgets table migration failed");
					e.printStackTrace();
				}
				
				//Enable foreign key checks
				Statement enableFKChecks = conMySQL.createStatement();
				enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");
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
				
				//Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

				// Copying data into new storage (MySQL)
				String query = " INSERT INTO categories (name, type, created_at, user_id)"
						+ " VALUES (?, ?, ?, ?)";
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				preparedStmt.setString(1, name);
				preparedStmt.setString(2, type);
				preparedStmt.setTimestamp(3, timeStamp);
				preparedStmt.setInt(4, userId);

				try {
					preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("categories table migration failed");
					e.printStackTrace();
				}
				
				//Enable foreign key checks
				Statement enableFKChecks = conMySQL.createStatement();
				enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");
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
				Timestamp lastRun = result.getTimestamp("last_run");
				LOGGER.debug("last_run" + lastRun );
				Timestamp timeStamp = result.getTimestamp("created_at");
				LOGGER.debug("created_at" + timeStamp );
				int budgetTypeId = result.getInt("budget_type_id");
				LOGGER.debug("budget_type_id" + budgetTypeId);
				String remark = result.getString("remark");
				LOGGER.debug("remark:" + remark);
				
				//Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

				// Copying data into new storage (MySQL)
				String query = " INSERT INTO recurrings (amount, type, last_run, created_at, budget_type_id)"
						+ " VALUES (?, ?, ?, ?, ?)";
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				preparedStmt.setDouble(1, amount);
				preparedStmt.setString(2, type);
				preparedStmt.setTimestamp(3, lastRun);
				preparedStmt.setTimestamp(4, timeStamp);
				preparedStmt.setInt(5, budgetTypeId);

				try {
					preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("recurrings table migration failed");
					e.printStackTrace();
				}
				
				//Enable foreign key checks
				Statement enableFKChecks = conMySQL.createStatement();
				enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");
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
				
				//Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

				// Copying data into new storage (MySQL)
				String query = " INSERT INTO recurrings (name, amount, remark, auto, transaction_on, created_at, budget_id, recurring_id)"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				preparedStmt.setString(1, name);
				preparedStmt.setDouble(2, amount);
				preparedStmt.setString(3, remark);
				preparedStmt.setBoolean(4, auto);
				preparedStmt.setTimestamp(5, transactionOn);
				preparedStmt.setTimestamp(6, timeStamp);
				preparedStmt.setInt(7, budgetId);
				preparedStmt.setInt(8, recurringId);
				
				try {
					preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("transactions table migration failed");
					e.printStackTrace();
				}
				
				//Enable foreign key checks
				Statement enableFKChecks = conMySQL.createStatement();
				enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");
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
//		forkliftUsers();
//		forkliftBudgetTypes();
//		forkliftBudgets();
//		forkliftCategories();
//		forkliftRecurrings();
		forkliftTransactions();
		LOGGER.info("**************Forklift Deactive***************");
		LOGGER.info("**************Transfer Stopped****************");
	}
	
	/**
	 * Compares the contents of the transaction tables
	 */
	public void checkTransactions(){
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM transactions");
			
			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM transactions");
			
			
			while(resultPostgres.next()) {
				
				//local variable so that objects may be passed properly
				int timeStampUpdateIndicator=0;
				
				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				String name_Postgres = resultPostgres.getString("name");
				double amount_Postgres = resultPostgres.getDouble("amount");
				String remark_Postgres = resultPostgres.getString("remark");
				boolean auto_Postgres = resultPostgres.getBoolean("auto");
				Timestamp transactionOn_Postgres = resultPostgres.getTimestamp("transaction_on");
				Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int budgetId_Postgres = resultPostgres.getInt("budget_id");
				int recurringId_Postgres = resultPostgres.getInt("recurring_id");
				
				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String name_MySQL = resultMySQL.getString("name");
				double amount_MySQL = resultMySQL.getDouble("amount");
				String remark_MySQL = resultMySQL.getString("remark");
				boolean auto_MySQL = resultMySQL.getBoolean("auto");
				Timestamp transactionOn_MySQL = resultMySQL.getTimestamp("transaction_on");
				Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int budgetId_MySQL = resultMySQL.getInt("budget_id");
				int recurringId_MySQL = resultMySQL.getInt("recurring_id");
				
				// Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

				String query = "";
				
				// Comparing values
				if(name_Postgres.equals(name_MySQL)){
					LOGGER.debug("name inconsistency: expected '"+name_Postgres+"' but received '"+name_MySQL+"'");
					query+= " UPDATE recurrings SET name = '"+name_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(amount_Postgres == amount_MySQL){
					LOGGER.debug("amount inconsistency: expected "+amount_Postgres+" but received "+amount_MySQL);
					query+= " UPDATE recurrings SET amount = "+amount_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(remark_Postgres.equals(remark_MySQL)){
					LOGGER.debug("remark inconsistency: expected '"+remark_Postgres+"' but received '"+remark_MySQL+"'");
					query+= " UPDATE recurrings SET name = '"+remark_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(auto_Postgres == auto_MySQL){
					LOGGER.debug("auto inconsistency: expected "+auto_Postgres+" but received "+auto_MySQL);
					query+= " UPDATE recurrings SET auto = "+auto_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(transactionOn_Postgres.equals(transactionOn_MySQL)){
					LOGGER.debug("transaction_on inconsistency: expected "+transactionOn_Postgres.getTime()+" but received "+transactionOn_MySQL.getTime());
					query+= " UPDATE recurrings SET transaction_on = ? WHERE id = "+id_MySQL+";";
					timeStampUpdateIndicator = 2;
				}
				if(timeStamp_Postgres.equals(timeStamp_MySQL)){
					LOGGER.debug("created_at inconsistency: expected "+timeStamp_Postgres+" but received "+timeStamp_MySQL);
					query+= " UPDATE recurrings SET created_at = ? WHERE id = "+id_MySQL+";";
					timeStampUpdateIndicator++;
				}
				if(budgetId_Postgres == budgetId_MySQL){
					LOGGER.debug("budget_id inconsistency: expected "+budgetId_Postgres+" but received "+budgetId_MySQL);
					query+= " UPDATE recurrings SET budget_id = "+budgetId_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(recurringId_Postgres == recurringId_MySQL){
					LOGGER.debug("recurring_id inconsistency: expected "+recurringId_Postgres+" but received "+recurringId_MySQL);
					query+= " UPDATE recurrings SET recurring_id = "+recurringId_Postgres+" WHERE id = "+id_MySQL+";";
				}
				
				
				// taking all of the MySQL queries and executing them
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				
				//Adding in any timestamp variables which require modification
				switch (timeStampUpdateIndicator){
				case 0: break;
				case 1: preparedStmt.setTimestamp(1, timeStamp_Postgres); break;
				case 2: preparedStmt.setTimestamp(1, transactionOn_Postgres); break;
				case 3:
					preparedStmt.setTimestamp(1, transactionOn_Postgres);
					preparedStmt.setTimestamp(2, timeStamp_Postgres);
				}
				
				
				try {
					preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("transactions table checking failed");
					e.printStackTrace();
				}
				
				//Enable foreign key checks
				Statement enableFKChecks = conMySQL.createStatement();
				enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");
				
				timeStampUpdateIndicator = 0;
			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Compares all of the forklifted data and corrects any inconsistencies.
	 */
	public void checkAll(){
		LOGGER.info("*********Checking Database Contents***********");
//		LOGGER.info("***************Checking Users*****************");
//		forkliftUsers();
//		LOGGER.info("************Checking Budget Types*************");
//		forkliftBudgetTypes();
//		LOGGER.info("**************Checking Budgets****************");
//		forkliftBudgets();
//		LOGGER.info("*************Checking Categories**************");
//		forkliftCategories();
//		LOGGER.info("*************Checking Recurrings**************");
//		forkliftRecurrings();
		LOGGER.info("************Checking Transactions*************");
		checkTransactions();
		LOGGER.info("*********Database Checking Complete***********");
	}

	@Test
	public void migrationTest() {

		// forklift the data from old storage to new 
		forklift();

		// check for inconsistencies 
		checkAll();
		// ensure inconsistencies are fixed 

		//shadow writes 

		// shadow reads for validation 


	}

}