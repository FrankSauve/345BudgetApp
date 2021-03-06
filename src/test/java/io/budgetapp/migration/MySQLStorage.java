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
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class MySQLStorage{

	private static final Logger LOGGER = LoggerFactory.getLogger(MigrationTest.class);

	private Connection conMySQL;
	
	int inconsistencies = 0;

	MySQLStorage(Connection con){
		conMySQL = con;
	}
	
	public int getReadInconsistencies() {
		return inconsistencies;
	}

	/**
	 * Insert into user table
	 */
	public void insertUsers(String username, String password, String name, Timestamp timeStamp, String currency) {

		try {

			// Inserting data
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

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Insert into budget_types table
	 */
	public void insertBudgetTypes(Timestamp timeStamp) {
		try {
			// Inserting data
			String query = " INSERT INTO budget_types (created_at) VALUES (?)";
			PreparedStatement preparedStmt = conMySQL.prepareStatement(query);
			preparedStmt.setTimestamp(1, timeStamp);

			try {
				preparedStmt.execute();
			}
			catch (SQLIntegrityConstraintViolationException  e) {
				LOGGER.error("budget_types table insertion failed");
				e.printStackTrace();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Insert into the Budgets table
	 */
	public void insertBudgets(String name, double projected, double actual, Date periodOn, Timestamp timeStamp, int userId, int categoryId, int typeId) {
		try {

			//Disable foreign key checks
			Statement disableFKChecks = conMySQL.createStatement();
			disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

			// Inserting data
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
				LOGGER.error("budgets table insertion failed");
				e.printStackTrace();
			}

			//Enable foreign key checks
			Statement enableFKChecks = conMySQL.createStatement();
			enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Insert into categories table
	 */
	public void insertCategories(int id, String name, String type, Timestamp timeStamp, int userId) {
		try {

			//Disable foreign key checks
			Statement disableFKChecks = conMySQL.createStatement();
			disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

			// Inserting data
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
				LOGGER.error("categories table insertion failed");
				e.printStackTrace();
			}

			//Enable foreign key checks
			Statement enableFKChecks = conMySQL.createStatement();
			enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Insert into the recurrings tables
	 */
	public void insertRecurrings(double amount, String type, Timestamp lastRun, Timestamp timeStamp, int budgetTypeId, String remark){
		try {

			//Disable foreign key checks
			Statement disableFKChecks = conMySQL.createStatement();
			disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

			// Inserting data 
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
				LOGGER.error("recurrings table insertion failed");
				e.printStackTrace();
			}

			//Enable foreign key checks
			Statement enableFKChecks = conMySQL.createStatement();
			enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Insert the transactions table
	 */
	public void insertTransactions(String name, double amount, String remark, boolean auto, Timestamp transactionOn, Timestamp timeStamp, int budgetId, int recurringId) {
		try {

			//Disable foreign key checks
			Statement disableFKChecks = conMySQL.createStatement();
			disableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=0");

			// Inserting data 
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
				LOGGER.error("transactions table insertion failed");
				e.printStackTrace();
			}

			//Enable foreign key checks
			Statement enableFKChecks = conMySQL.createStatement();
			enableFKChecks.executeQuery("SET FOREIGN_KEY_CHECKS=1");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


}