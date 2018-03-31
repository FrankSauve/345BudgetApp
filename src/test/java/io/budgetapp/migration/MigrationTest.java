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
	
	public void forklift() {
		LOGGER.info("**************Forklift Active***************");
		LOGGER.info("**************Tranferring Data***************");
		forkliftUsers();
		forkliftBudgetTypes();
		forkliftBudgets();
		LOGGER.info("**************Forklift Deactive***************");
		LOGGER.info("**************Transfer Stopped****************");
	}


	@Test
	public void migrationTest() {

		// forklift the data from old storage to new 
		forklift();

		// check for inconsistencies 

		// ensure inconsistencies are fixed 

		//shadow writes 

		// shadow reads for validation 


	}

}