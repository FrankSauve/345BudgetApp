package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

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
	 * Migrate data from postgres to mysql
	 */
	public void forklift() {
		LOGGER.info("**************Forklift Active***************");
		LOGGER.info("**************Tranferring Data***************");
		try {
			Connection conPostgres = DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			ForkLifter forkLifter = new ForkLifter(conPostgres, conMySQL);
			forkLifter.forkliftUsers();
			forkLifter.forkliftBudgetTypes();
			forkLifter.forkliftBudgets();
			forkLifter.forkliftCategories();
			forkLifter.forkliftRecurrings();
			forkLifter.forkliftTransactions();

			forkLifter.close();

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
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

	private void shadowWrite() {

		try {
			Connection conPostgres = DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			ShadowWriter shadowWriter = new ShadowWriter(conPostgres, conMySQL);

			shadowWriter.connectToDatabases();

			shadowWriter.shadowWriteUser();
			shadowWriter.shadowWriteBudgetType();
			shadowWriter.shadowWriteBudget();
			shadowWriter.shadowWriteCategories();
			shadowWriter.shadowWriteRecurrings();

			shadowWriter.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	private ShadowReader shadowReader() {
		
		ShadowReader shadowReader = null;

		try {
			Connection conPostgres = DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);

			shadowReader = new ShadowReader(conPostgres, conMySQL);

			shadowReader.connectToDatabases();

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return shadowReader;

	}


	@Test
	public void migrationTest() {

		// forklift the data from old storage to new 
		forklift();

		// check for inconsistencies and fixed them
		checkAll();


		//Start consistency checker asynchronously
		new Thread( new Runnable() {
			public void run(){
				checkAll();
				return; 
			}
		}).start();


		//shadow writes
		shadowWrite();

		//shadow reads for validation
		/*ShadowReader shadowReader = shadowReader(); 
		Assert.assertEquals(5, shadowReader.shadowReadUser());
		Assert.assertEquals(0, shadowReader.shadowReadUser());*/


	}

}