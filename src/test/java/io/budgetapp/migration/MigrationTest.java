package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.ResultSet;

import org.junit.Test;


public class MigrationTest{

	String host1 = "jdbc:postgresql://localhost:5432/345BudgetApp";
	String username1 = "postgres";
	String password1 = "postgres";

	String host2 = "jdbc:mysql://localhost:3306/345BudgetApp";
	String username2 = "root";
	String password2 = "root";

	public void forklift() {

		System.out.println("**************Forklift Active***************");
		System.out.println("**************Tranferring Data***************");
		try {
			Connection conPostgres =  DriverManager.getConnection(host1,username1,password1);
			Connection conMySQL = DriverManager.getConnection(host2,username2,password2);


			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet result = stmtPostgres.executeQuery("SELECT * FROM users");

			while(result.next()) {

				// Getting values from the old database (Postgres)
				int id = result.getInt("id");
				System.out.println("id: " + id);

				String username= result.getString("username");
				System.out.println("username: " + username);
				String password = result.getString("password");
				System.out.println("password: " + password);
				String name = result.getString("name");
				System.out.println("name: " + name);
				Timestamp timeStamp = result.getTimestamp("created_at");
				System.out.println("created_at" + timeStamp );
				String currency = result.getString("currency");
				System.out.println("currency:" + currency);


				// Copying data into new storage (MySQL)
				String query = " insert into users (username, password, name, created_at, currency)"
						+ " values (?, ?, ?, ?, ?)";
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
					System.out.println();
					System.out.println("username " + username + " already exists");
					System.out.println();
				}
			}

			conMySQL.close();
			conPostgres.close();
		} catch (SQLException e) {
			e.printStackTrace();

		}
		System.out.println("**************Forklift Deactive***************");
		System.out.println("**************Tranfer Stopped****************");
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