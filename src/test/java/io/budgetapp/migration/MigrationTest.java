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
				Timestamp lastRun = result.getTimestamp("last_run_at");
				LOGGER.debug("last_run_at" + lastRun );
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
				String query = " INSERT INTO recurrings (amount, type, last_run_at, created_at, budget_type_id)"
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
				String query = " INSERT INTO transactions (name, amount, remark, auto, transaction_on, created_at, budget_id, recurring_id)"
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
		//		forkliftTransactions();
		LOGGER.info("**************Forklift Deactive***************");
		LOGGER.info("**************Transfer Stopped****************");
	}

	/**
	 * Compares the user tables
	 */
	public void checkUsers(){
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM users");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM users");

			while(resultPostgres.next() && resultMySQL.next()) {

				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				String username_Postgres= resultPostgres.getString("username");
				String password_Postgres = resultPostgres.getString("password");
				String name_Postgres = resultPostgres.getString("name");
				Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				String currency_Postgres = resultPostgres.getString("currency");

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String username_MySQL= resultMySQL.getString("username");
				String password_MySQL = resultMySQL.getString("password");
				String name_MySQL = resultMySQL.getString("name");
				Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				String currency_MySQL = resultMySQL.getString("currency");


				// Copying data into new storage (MySQL)
				String query = "";
				boolean hasTimeStampInconsistency = false;

				//Comparing Values
				if(!username_Postgres.equals(username_MySQL)){
					LOGGER.debug("username inconsistency: expected '"+username_Postgres+"' but received '"+username_MySQL+"'");
					query+= " UPDATE users SET username = '"+username_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(!password_Postgres.equals(password_MySQL)){
					LOGGER.debug("password inconsistency: expected '"+password_Postgres+"' but received '"+password_MySQL+"'");
					query+= " UPDATE users SET password = '"+password_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(!name_Postgres.equals(name_MySQL)){
					LOGGER.debug("name inconsistency: expected '"+name_Postgres+"' but received '"+name_MySQL+"'");
					query+= " UPDATE users SET name = '"+name_Postgres+"' WHERE id = "+id_MySQL+";";
				}


				String timeStampPostgres = timeStamp_Postgres.toString().split("\\.")[0];
				String timeStampMySQL = timeStamp_MySQL.toString().split("\\.")[0];

				if(!timeStampPostgres.equals(timeStampMySQL)){
					LOGGER.debug("created_at inconsistency: expected "+timeStamp_Postgres.getTime()+" but received "+timeStamp_MySQL.getTime() + " at index " + id_MySQL);
					query+= " UPDATE users SET created_at = ? WHERE id = "+id_MySQL+";";
					hasTimeStampInconsistency = true;
				}

				if (currency_Postgres == null && currency_MySQL == null) {
					break;
				}
				else if(!currency_Postgres.equals(currency_MySQL)){
					LOGGER.debug("currency inconsistency: expected '"+currency_Postgres+"' but received '"+currency_MySQL+"'");
					query+= " UPDATE users SET currency = '"+currency_Postgres+"' WHERE id = "+id_MySQL+";";
				}

				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);

				//Adding in any unspecified variables to the statement if they require updating
				if(hasTimeStampInconsistency){
					preparedStmt.setTimestamp(1, timeStamp_Postgres);
				}

				try {
					if(query != "")
						preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("users table checking failed");
				}
				hasTimeStampInconsistency = false;
			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Compares the budget_types data
	 */
	public void checkBudgetTypes(){
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM budget_types");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM budget_types");

			while(resultPostgres.next() && resultMySQL.next()) {

				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");


				String query = "";
				boolean hasTimeStampInconsistency = false;

				// Extract date and time only 
				String timeStampPostgres = timeStamp_Postgres.toString().split("\\.")[0];
				String timeStampMySQL = timeStamp_MySQL.toString().split("\\.")[0];

				if(!timeStampPostgres.equals(timeStampMySQL)){
					LOGGER.debug("created_at inconsistency: expected "+timeStamp_Postgres.getTime()+" but received "+timeStamp_MySQL.getTime() + " at index " + id_MySQL);
					query+= " UPDATE budget_types SET created_at = ? WHERE id = "+id_MySQL+";";
					hasTimeStampInconsistency = true;
				}

				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				//Adding in any unspecified variables to the statement if they require updating
				if(hasTimeStampInconsistency){
					preparedStmt.setTimestamp(1, timeStamp_Postgres);
					try {
						if(query != "")
							preparedStmt.execute();
					}
					catch (SQLIntegrityConstraintViolationException  e) {
						LOGGER.error("budget_types table checking failed");
						e.printStackTrace();
					}
					hasTimeStampInconsistency = false;
				}

			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Compares the contents of the budgets table
	 */
	public void checkBudgets(){
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM budgets");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM budgets");

			while(resultPostgres.next()) {

				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				String name_Postgres = resultPostgres.getString("name");
				double projected_Postgres = resultPostgres.getDouble("projected");
				double actual_Postgres = resultPostgres.getDouble("actual");
				Date periodOn_Postgres= resultPostgres.getDate("period_on");
				Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int userId_Postgres = resultPostgres.getInt("user_id");
				int categoryId_Postgres = resultPostgres.getInt("category_id");
				int typeId_Postgres = resultPostgres.getInt("type_id");

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String name_MySQL = resultMySQL.getString("name");
				double projected_MySQL = resultMySQL.getDouble("projected");
				double actual_MySQL = resultMySQL.getDouble("actual");
				Date periodOn_MySQL= resultMySQL.getDate("period_on");
				Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int userId_MySQL = resultMySQL.getInt("user_id");
				int categoryId_MySQL = resultMySQL.getInt("category_id");
				int typeId_MySQL = resultMySQL.getInt("type_id");

				//Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");


				// Copying data into new storage (MySQL)
				String query = "";
				boolean hasTimeStampInconsistency =false;
				boolean hasPeriodOnInconsistency = false;

				//Comparing Values
				if(!name_Postgres.equals(name_MySQL)){
					LOGGER.debug("name inconsistency: expected '"+name_Postgres+"' but received '"+name_MySQL+"'");
					query+= " UPDATE budgets SET name = '"+name_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(projected_Postgres != projected_MySQL){
					LOGGER.debug("projected inconsistency: expected "+projected_Postgres+" but received "+projected_MySQL);
					query+= " UPDATE budgets SET projected = "+projected_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(actual_Postgres != actual_MySQL){
					LOGGER.debug("actual inconsistency: expected "+actual_Postgres+" but received "+actual_MySQL);
					query+= " UPDATE budgets SET actual = "+actual_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(!periodOn_Postgres.equals(periodOn_MySQL)){
					LOGGER.debug("period_on inconsistency: expected "+periodOn_Postgres.getTime()+" but received "+periodOn_MySQL.getTime());
					query+= " UPDATE budgets SET period_on = ? WHERE id = "+id_MySQL+";";

					hasPeriodOnInconsistency = true;
				}
				if(!timeStamp_Postgres.equals(timeStamp_MySQL)){
					LOGGER.debug("created_at inconsistency: expected "+timeStamp_Postgres.getTime()+" but received "+timeStamp_MySQL.getTime());
					query+= " UPDATE budgets SET created_at = ? WHERE id = "+id_MySQL+";";

					hasTimeStampInconsistency =true;
				}
				if(userId_Postgres != userId_MySQL){
					LOGGER.debug("user_id inconsistency: expected "+userId_Postgres+" but received "+userId_MySQL);
					query+= " UPDATE budgets SET user_id = "+userId_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(categoryId_Postgres != categoryId_MySQL){
					LOGGER.debug("category_id inconsistency: expected "+categoryId_Postgres+" but received "+categoryId_MySQL);
					query+= " UPDATE budgets SET category_id = "+categoryId_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(typeId_Postgres != typeId_MySQL){
					LOGGER.debug("type_id inconsistency: expected "+typeId_Postgres+" but received "+typeId_MySQL);
					query+= " UPDATE budgets SET type_id = "+typeId_Postgres+" WHERE id = "+id_MySQL+";";
				}

				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);

				if(hasPeriodOnInconsistency && hasTimeStampInconsistency) {
					preparedStmt.setDate(1, periodOn_Postgres);
					preparedStmt.setTimestamp(2, timeStamp_Postgres);
				}
				else if (hasPeriodOnInconsistency && !hasTimeStampInconsistency) {
					preparedStmt.setDate(1, periodOn_Postgres);
				}
				else if (!hasPeriodOnInconsistency && hasTimeStampInconsistency) {
					preparedStmt.setTimestamp(1, timeStamp_Postgres);
				}


				try {
					if(query != "")
						preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("budgets table checking failed");
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
	}	/**
	 * Compares the contents of the categories table
	 */
	public void checkCategories(){
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM categories");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM categories");

			while(resultPostgres.next() && resultMySQL.next()) {

				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				String name_Postgres = resultPostgres.getString("name");
				String type_Postgres = resultPostgres.getString("type");
				Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int userId_Postgres = resultPostgres.getInt("user_id");

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String name_MySQL = resultMySQL.getString("name");
				String type_MySQL = resultMySQL.getString("type");
				Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int userId_MySQL = resultMySQL.getInt("user_id");

				//Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

				// Copying data into new storage (MySQL)
				String query = "";
				boolean hasTimeStampInconsistency = false;

				//Comparing Values
				if(!name_Postgres.equals(name_MySQL)){
					LOGGER.debug("name inconsistency: expected '"+name_Postgres+"' but received '"+name_MySQL+"'");
					query+= " UPDATE categories SET name = '"+name_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(!type_Postgres.equals(type_MySQL)){
					LOGGER.debug("type inconsistency: expected '"+type_Postgres+"' but received '"+type_MySQL+"'");
					query+= " UPDATE categories SET type = '"+type_Postgres+"' WHERE id = "+id_MySQL+";";
				}

				// Extract date and time only 
				String timeStampPostgres = timeStamp_Postgres.toString().split("\\.")[0];
				String timeStampMySQL = timeStamp_MySQL.toString().split("\\.")[0];

				if(!timeStampPostgres.equals(timeStampMySQL)){
					LOGGER.debug("created_at inconsistency: expected "+timeStamp_Postgres.getTime()+" but received "+timeStamp_MySQL.getTime() + " at index " + id_MySQL);
					query+= " UPDATE categories SET created_at = ? WHERE id = "+id_MySQL+";";
					hasTimeStampInconsistency = true;
				}
				if(userId_Postgres != userId_MySQL){
					LOGGER.debug("user_id inconsistency: expected "+userId_Postgres+" but received "+userId_MySQL);
					query+= " UPDATE categories SET user_id = "+userId_Postgres+" WHERE id = "+id_MySQL+";";
				}


				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
				//adding in the Timestamp variable to the statement if it was inconsistent
				if(hasTimeStampInconsistency){
					preparedStmt.setTimestamp(1, timeStamp_Postgres);
				}

				try {
					if(query != "")
						preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("categories table checking failed");
					e.printStackTrace();
				}

				//Enable foreign key checks
				Statement enableFKChecks = conMySQL.createStatement();
				enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");
				hasTimeStampInconsistency = false;
			}
			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * Compares the contents of the recurring table
	 */
	public void checkRecurrings(){
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM recurrings");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM recurrings");

			while(resultPostgres.next() && resultMySQL.next()) {

				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				double amount_Postgres = resultPostgres.getDouble("amount");
				String type_Postgres = resultPostgres.getString("type");
				Timestamp lastRun_Postgres = resultPostgres.getTimestamp("last_run_at");
				Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int budgetTypeId_Postgres = resultPostgres.getInt("budget_type_id");
				String remark_Postgres = resultPostgres.getString("remark");

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				double amount_MySQL = resultMySQL.getDouble("amount");
				String type_MySQL = resultMySQL.getString("type");
				Timestamp lastRun_MySQL = resultMySQL.getTimestamp("last_run_at");
				Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int budgetTypeId_MySQL = resultMySQL.getInt("budget_type_id");
				String remark_MySQL = resultMySQL.getString("remark");

				//Disable foreign key checks
				Statement disableFKChecks = conMySQL.createStatement();
				disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

				String query ="";
				boolean lastRunInconsistency =false;
				boolean timeStampInconsistency = false;

				//Comparing values
				if(amount_Postgres != amount_MySQL){
					LOGGER.debug("amount inconsistency: expected "+amount_Postgres+" but received "+amount_MySQL);
					query+= " UPDATE recurrings SET amount = "+amount_Postgres+" WHERE id = "+id_MySQL+";";
				}

				if(!type_Postgres.equals(type_MySQL)){
					LOGGER.debug("type inconsistency: expected '"+type_Postgres+"' but received '"+type_MySQL+"'");
					query+= " UPDATE recurrings SET type = '"+type_Postgres+"' WHERE id = "+id_MySQL+";";
				}

				// Extract date and time only 
				String lastRunPostgres = lastRun_Postgres.toString().split("\\.")[0];
				String lastRunMySQL = lastRun_MySQL.toString().split("\\.")[0];

				if(!lastRunPostgres.equals(lastRunMySQL)){
					LOGGER.debug("last_run_at inconsistency: expected "+lastRun_Postgres.getTime()+" but received "+lastRun_MySQL.getTime());
					query+= " UPDATE recurrings SET last_run_at = ? WHERE id = "+id_MySQL+";";
					lastRunInconsistency = true;
				}

				// Extract date and time only 
				String timeStampPostgres = timeStamp_Postgres.toString().split("\\.")[0];
				String timeStampMySQL = timeStamp_MySQL.toString().split("\\.")[0];

				if(!timeStampPostgres.equals(timeStampMySQL)){
					LOGGER.debug("created_at inconsistency: expected "+timeStamp_Postgres.getTime()+" but received "+timeStamp_MySQL.getTime());
					query+= " UPDATE recurrings SET created_at = ? WHERE id = "+id_MySQL+";";
					timeStampInconsistency = true;
				}

				if(budgetTypeId_Postgres != budgetTypeId_MySQL){
					LOGGER.debug("budget_type_id inconsistency: expected "+budgetTypeId_Postgres+" but received "+budgetTypeId_MySQL);
					query+= " UPDATE recurrings SET budget_type_id = "+budgetTypeId_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(!remark_Postgres.equals(remark_MySQL)){
					LOGGER.debug("remark inconsistency: expected "+remark_Postgres+" but received '"+remark_MySQL+"'");
					query+= " UPDATE recurrings SET remark = "+remark_Postgres+" WHERE id = "+id_MySQL+";";
				}

				//forming all the queries into a statement
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);


				if(lastRunInconsistency && timeStampInconsistency) {
					preparedStmt.setTimestamp(1, lastRun_Postgres);
					preparedStmt.setTimestamp(2, timeStamp_Postgres);
				}
				else if (lastRunInconsistency && !timeStampInconsistency) {
					preparedStmt.setTimestamp(1, lastRun_Postgres);
				}
				else if (!lastRunInconsistency && timeStampInconsistency) {
					preparedStmt.setTimestamp(1, timeStamp_Postgres);
				}

				try {
					if(query != "")
						preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("recurrings table checking failed");
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
	 * Compares the contents of the transaction table
	 */
	public void checkTransactions(){
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM transactions");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM transactions");


			while(resultPostgres.next() & resultMySQL.next()) {

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
				boolean timeStampInconsistency = false;
				boolean transactionOnInconsistency =false;


				// Comparing values
				if(!name_Postgres.equals(name_MySQL)){
					LOGGER.debug("name inconsistency: expected '"+name_Postgres+"' but received '"+name_MySQL+"'");

					query+= " UPDATE transactions SET name = '"+name_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(amount_Postgres != amount_MySQL){
					LOGGER.debug("amount inconsistency: expected "+amount_Postgres+" but received "+amount_MySQL);
					query+= " UPDATE transactions SET amount = "+amount_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(!remark_Postgres.equals(remark_MySQL)){
					LOGGER.debug("remark inconsistency: expected '"+remark_Postgres+"' but received '"+remark_MySQL+"'");
					query+= " UPDATE transactions SET remark = '"+remark_Postgres+"' WHERE id = "+id_MySQL+";";
				}
				if(auto_Postgres != auto_MySQL){
					LOGGER.debug("auto inconsistency: expected "+auto_Postgres+" but received "+auto_MySQL);
					query+= " UPDATE transactions SET auto = "+auto_Postgres+" WHERE id = "+id_MySQL+";";
				}
				
				// Extract date and time only 
				String transactionOnPostgres = transactionOn_Postgres.toString().split("\\.")[0];
				String transactionOnMySQL = transactionOn_MySQL.toString().split("\\.")[0];
				
				if(!transactionOnPostgres.equals(transactionOnMySQL)){
					LOGGER.debug("transaction_on inconsistency: expected "+transactionOn_Postgres.getTime()+" but received "+transactionOn_MySQL.getTime());
					query+= " UPDATE transactions SET transaction_on = ? WHERE id = "+id_MySQL+";";

					transactionOnInconsistency = true;
				}
				
				
				// Extract date and time only 
				String timeStampPostgres = timeStamp_Postgres.toString().split("\\.")[0];
				String timeStampMySQL = timeStamp_MySQL.toString().split("\\.")[0];
				
				if(!timeStampPostgres.equals(timeStampMySQL)){
					LOGGER.debug("created_at inconsistency: expected "+timeStamp_Postgres.getTime()+" but received "+timeStamp_MySQL.getTime());
					query+= " UPDATE transactions SET created_at = ? WHERE id = "+id_MySQL+";";
					timeStampInconsistency = true;
				}
				if(budgetId_Postgres != budgetId_MySQL){
					LOGGER.debug("budget_id inconsistency: expected "+budgetId_Postgres+" but received "+budgetId_MySQL);
					query+= " UPDATE transactions SET budget_id = "+budgetId_Postgres+" WHERE id = "+id_MySQL+";";
				}
				if(recurringId_Postgres != recurringId_MySQL){
					LOGGER.debug("recurring_id inconsistency: expected "+recurringId_Postgres+" but received "+recurringId_MySQL);
					query+= " UPDATE transactions SET recurring_id = "+recurringId_Postgres+" WHERE id = "+id_MySQL+";";
				}


				// taking all of the MySQL queries and executing them
				PreparedStatement preparedStmt = conMySQL.prepareStatement(query);


				if(transactionOnInconsistency && timeStampInconsistency) {
					preparedStmt.setTimestamp(1, transactionOn_Postgres);
					preparedStmt.setTimestamp(2, timeStamp_Postgres);
				}
				else if (transactionOnInconsistency && !timeStampInconsistency) {
					preparedStmt.setTimestamp(1, transactionOn_Postgres);
				}
				else if (!transactionOnInconsistency && timeStampInconsistency) {
					preparedStmt.setTimestamp(1, timeStamp_Postgres);
				}

				try {
					if(query != "")
						preparedStmt.execute();
				}
				catch (SQLIntegrityConstraintViolationException  e) {
					LOGGER.error("transactions table checking failed");
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
	 * Compares all of the forklifted data and corrects any inconsistencies.
	 */
	public void checkAll(){
		LOGGER.info("*********Checking Database Contents***********");
		LOGGER.info("***************Checking Users*****************");
		//checkUsers();
		LOGGER.info("************Checking Budget Types*************");
		//checkBudgetTypes();
		LOGGER.info("**************Checking Budgets****************");
		//checkBudgets();
		LOGGER.info("*************Checking Categories**************");
		//checkCategories();
		LOGGER.info("*************Checking Recurrings**************");
		//checkRecurrings();
		LOGGER.info("************Checking Transactions*************");
		checkTransactions();
		LOGGER.info("*********Database Checking Complete***********");
	}

	@Test
	public void migrationTest() {

		// forklift the data from old storage to new 
		forklift();

		// check for inconsistencies 
		// ensure inconsistencies are fixed 
		checkAll();



		//shadow writes 

		// shadow reads for validation 


	}

}