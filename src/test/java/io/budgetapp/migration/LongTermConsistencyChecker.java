package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongTermConsistencyChecker {

	private long inconsistencies;
	private Connection conPostgres;
	private Connection conMySQL;
	private HashMap<Integer, Integer> userMap = new HashMap<>();
	private HashMap<Integer, Integer> budgetMap = new HashMap<>();

	LongTermConsistencyChecker(Connection conPostgres, Connection conMySQL){
		this.conPostgres = conPostgres;
		this.conMySQL = conMySQL;
		inconsistencies = 0;
	}

	public long getNumInconsistencies(){
		return inconsistencies;
	}	

	public void close() throws SQLException {
		conMySQL.close();
		conPostgres.close();
		inconsistencies = 0;
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(ConsistencyChecker.class);
	public void addUsers()	{

		try {
			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM users");


			while(resultPostgres.next()) {

				// Hashing values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				String username_Postgres= resultPostgres.getString("username");
				String password_Postgres = resultPostgres.getString("password");
				String name_Postgres = resultPostgres.getString("name");
				Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				String currency_Postgres = resultPostgres.getString("currency");

				int userHash = username_Postgres.hashCode() + password_Postgres.hashCode() + 
						name_Postgres.hashCode() + timeStamp_Postgres.hashCode() + 
						currency_Postgres.hashCode();

				userMap.put(id_Postgres, userHash);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		//		System.out.println(userMap.entrySet());


	}
}
