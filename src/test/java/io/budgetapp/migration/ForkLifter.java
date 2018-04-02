package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForkLifter {

	private Connection conPostgres;
	private Connection conMySQL;
	MySQLStorage mySQLStorage;

	private static final Logger LOGGER = LoggerFactory.getLogger(ForkLifter.class);

	ForkLifter(Connection conPostgres, Connection conMySQL){
		this.conPostgres = conPostgres;
		this.conMySQL = conMySQL;
	}

	public void close() throws SQLException {
		conMySQL.close();
		conPostgres.close();
	}

	/**
	 * Migrates the user table
	 */
	public void forkliftUsers() {
		LOGGER.info("**************Forklift Users***************");
		try {

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
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}



}
