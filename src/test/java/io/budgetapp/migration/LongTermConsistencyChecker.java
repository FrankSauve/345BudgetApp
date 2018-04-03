package io.budgetapp.migration;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongTermConsistencyChecker {

	private long inconsistencies;
	private Connection conPostgres;
	private Connection conMySQL;
	//private HashMap<Integer, Integer> userMap = new HashMap<>();
	private HashMap<Integer, Integer> budgetMapPostgres = new HashMap<>();
	//private HashMap<Integer, Integer> budgetMapMySQL = new HashMap<>(); 
	private HashMap<Integer, Integer> categoriesMapPostgres = new HashMap<>(); 
	//private HashMap<Integer, Integer> categoriesMapMySQL = new HashMap<>(); 
	private HashMap<Integer, Integer> recurringsMapPostgres = new HashMap<>(); 
	//private HashMap<Integer, Integer> recurringsMapMySQL = new HashMap<>(); 
	private HashMap<Integer, Integer> transactionsMapPostgres = new HashMap<>(); 
	//private HashMap<Integer, Integer> transactionsMapMySQL = new HashMap<>(); 


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

	private static final Logger LOGGER = LoggerFactory.getLogger(LongTermConsistencyChecker.class);
	
	public void checkUsers() {

		LOGGER.info("***************Checking Users*****************");

		try {

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
				
				//Putting values form the old database to hash map
				int userHash_Postgres = username_Postgres.hashCode() + password_Postgres.hashCode() + 
						name_Postgres.hashCode();

				userMap.put(id_Postgres, userHash_Postgres);

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String username_MySQL= resultMySQL.getString("username");
				String password_MySQL = resultMySQL.getString("password");
				String name_MySQL = resultMySQL.getString("name");
				Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				String currency_MySQL = resultMySQL.getString("currency");

				int userHash_MySQL = username_MySQL.hashCode() + password_MySQL.hashCode() + 
						name_MySQL.hashCode();


				//Comparing Values  !timeStampPostgres.equals(timeStampMySQL)

				if(! (userMap.get((Integer)id_MySQL)).equals((Integer)userHash_MySQL) ){
					LOGGER.debug("user hash inconsistency: expected '"+ userMap.get(id_MySQL) +"' but received '"+ userHash_MySQL +"'");

					inconsistencies++;
				}

			} }catch (SQLException e) {
				e.printStackTrace();
			}
	}

	public void checkBudgets(){
		LOGGER.info("***************Checking Budgets*****************");
		try{
			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM budgets");
			
			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM budgets");

			while(resultPostgres.next() && resultMySQL.next()) {

				//Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				String name_Postgres = resultPostgres.getString("name");
				double projected_Postgres = resultPostgres.getDouble("projected");
				double actual_Postgres = resultPostgres.getDouble("actual");
				Date periodOn_Postgres= resultPostgres.getDate("period_on");
				//Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int userId_Postgres = resultPostgres.getInt("user_id");
				int categoryId_Postgres = resultPostgres.getInt("category_id");
				int typeId_Postgres = resultPostgres.getInt("type_id");
				
				String projected_P = String.valueOf( (int)projected_Postgres ) ;
				String actual_P = String.valueOf( (int)actual_Postgres ); 
				String userId_P = String.valueOf(userId_Postgres); 
				String categoryId_P = String.valueOf(categoryId_Postgres); 
				String typeId_P = String.valueOf(typeId_Postgres); 

				//Putting values form the old database to hash map
				int budgetHashP = name_Postgres.hashCode() + projected_P.hashCode() + actual_P.hashCode() + 
						periodOn_Postgres.toString().hashCode() + 
						userId_P.hashCode() + categoryId_P.hashCode() + typeId_P.hashCode(); 
				
				budgetMapPostgres.put(id_Postgres, budgetHashP); 
				
				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String name_MySQL = resultMySQL.getString("name");
				double projected_MySQL = resultMySQL.getDouble("projected");
				double actual_MySQL = resultMySQL.getDouble("actual");
				Date periodOn_MySQL= resultMySQL.getDate("period_on");
				// Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int userId_MySQL = resultMySQL.getInt("user_id");
				int categoryId_MySQL = resultMySQL.getInt("category_id");
				int typeId_MySQL = resultMySQL.getInt("type_id");
				
				String projected_M = String.valueOf( (int)projected_MySQL) ;
				String actual_M = String.valueOf( (int)actual_MySQL ); 
				String userId_M = String.valueOf(userId_MySQL); 
				String categoryId_M = String.valueOf(categoryId_MySQL); 
				String typeId_M = String.valueOf(typeId_MySQL);
				
				//Putting values form the old database to hash map
				int budgetHashM = name_MySQL.hashCode() + projected_M.hashCode() + actual_M.hashCode() + 
						periodOn_MySQL.toString().hashCode() +  
						userId_M.hashCode() + categoryId_M.hashCode() + typeId_M.hashCode(); 
				 
				//budgetMapMySQL.put(id_MySQL, budgetHashM);
				
				if(! (budgetMapPostgres.get((Integer)id_MySQL)).equals((Integer)budgetHashM) ){
					LOGGER.debug("budget hash inconsistency: expected '"+ budgetMapPostgres.get(id_MySQL) +"' but received '"+ budgetHashM +"'");

					inconsistencies++;
				}
				
				//if(!(budgetMapPostgres.get(id_Postgres)).equals(budgetMapMySQL.get(budgetHashM)));
				
			}
			
		}  catch (SQLException e) {
			e.printStackTrace();
	}
	
}

	
	public void checkCategories(){
		LOGGER.info("***************Checking Categories*****************");
		try {

			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM categories");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM categories");

			while(resultPostgres.next() && resultMySQL.next()) {

				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				String name_Postgres = resultPostgres.getString("name");
				String type_Postgres = resultPostgres.getString("type");
				//Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int userId_Postgres = resultPostgres.getInt("user_id");
				
				String userId_P = String.valueOf( (int)userId_Postgres );
				
				int categoriesHashP = name_Postgres.hashCode() + type_Postgres.hashCode() + userId_P.hashCode(); 
				
				categoriesMapPostgres.put(id_Postgres, categoriesHashP); 

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String name_MySQL = resultMySQL.getString("name");
				String type_MySQL = resultMySQL.getString("type");
				//Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int userId_MySQL = resultMySQL.getInt("user_id");
				
				String userId_M = String.valueOf( (int)userId_MySQL );
				
				int categoriesHashM = name_MySQL.hashCode() + type_MySQL.hashCode() + userId_M.hashCode(); 
				
				//categoriesMapMySQL.put(id_MySQL, categoriesHashM); 
				
				if(! (categoriesMapPostgres.get((Integer)id_MySQL)).equals((Integer)categoriesHashM) ){
					LOGGER.debug("categories hash inconsistency: expected '"+ categoriesMapPostgres.get(id_MySQL) +"' but received '"+ categoriesHashM +"'");

					inconsistencies++;
				}

			
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void checkRecurrings(){
		
		try {
			LOGGER.info("***************Checking Recurrings*****************");
			Statement stmtPostgres = conPostgres.createStatement( );
			ResultSet resultPostgres = stmtPostgres.executeQuery("SELECT * FROM recurrings");

			Statement stmtMySQL = conMySQL.createStatement( );
			ResultSet resultMySQL = stmtMySQL.executeQuery("SELECT * FROM recurrings");

			while(resultPostgres.next() && resultMySQL.next()) {

				// Getting values from the old database (Postgres)
				int id_Postgres = resultPostgres.getInt("id");
				double amount_Postgres = resultPostgres.getDouble("amount");
				String type_Postgres = resultPostgres.getString("type");
				//Timestamp lastRun_Postgres = resultPostgres.getTimestamp("last_run_at");
				//Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int budgetTypeId_Postgres = resultPostgres.getInt("budget_type_id");
				String remark_Postgres = resultPostgres.getString("remark");
				
				String amount_P = String.valueOf( (int)amount_Postgres );
				String budgetTypeId_P = String.valueOf( (int)budgetTypeId_Postgres );
				
				int recurringsHashP = amount_P.hashCode() + type_Postgres.hashCode() + budgetTypeId_P.hashCode() + remark_Postgres.hashCode(); 
				
				recurringsMapPostgres.put(id_Postgres, recurringsHashP); 
				

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				double amount_MySQL = resultMySQL.getDouble("amount");
				String type_MySQL = resultMySQL.getString("type");
				//Timestamp lastRun_MySQL = resultMySQL.getTimestamp("last_run_at");
				//Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int budgetTypeId_MySQL = resultMySQL.getInt("budget_type_id");
				String remark_MySQL = resultMySQL.getString("remark");
				
				String amount_M = String.valueOf( (int)amount_MySQL );
				String budgetTypeId_M = String.valueOf( (int)budgetTypeId_MySQL );
				
				int recurringsHashM = amount_M.hashCode() + type_MySQL.hashCode() + budgetTypeId_M.hashCode() + remark_MySQL.hashCode(); 
				
				//recurringsMapMySQL.put(id_MySQL, recurringsHashM); 
				
				if(! (recurringsMapPostgres.get((Integer)id_MySQL)).equals((Integer)recurringsHashM) ){
					LOGGER.debug("recurrings hash inconsistency: expected '"+ recurringsMapPostgres.get(id_MySQL) +"' but received '"+ recurringsHashM +"'");

					inconsistencies++;
				}
				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Compares the contents of the transaction table
	 */
	public void checkTransactions(){
		
		try {
			LOGGER.info("***************Checking Transactions*****************");
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
				//Timestamp transactionOn_Postgres = resultPostgres.getTimestamp("transaction_on");
				//Timestamp timeStamp_Postgres = resultPostgres.getTimestamp("created_at");
				int budgetId_Postgres = resultPostgres.getInt("budget_id");
				int recurringId_Postgres = resultPostgres.getInt("recurring_id");
				
				String amount_P = String.valueOf( (int)amount_Postgres );
				String budgetId_P = String.valueOf( (int)budgetId_Postgres );
				String recurringId_P = String.valueOf( (int)recurringId_Postgres );
				String auto_P = String.valueOf(auto_Postgres); 
				
				int transactionHashP = name_Postgres.hashCode() + amount_P.hashCode() + remark_Postgres.hashCode() + auto_P.hashCode() +
						budgetId_P.hashCode() + recurringId_P.hashCode(); 
				
				transactionsMapPostgres.put(id_Postgres, transactionHashP); 

				// Getting values from the new database (MySQL)
				int id_MySQL = resultMySQL.getInt("id");
				String name_MySQL = resultMySQL.getString("name");
				double amount_MySQL = resultMySQL.getDouble("amount");
				String remark_MySQL = resultMySQL.getString("remark");
				boolean auto_MySQL = resultMySQL.getBoolean("auto");
				//Timestamp transactionOn_MySQL = resultMySQL.getTimestamp("transaction_on");
				//Timestamp timeStamp_MySQL = resultMySQL.getTimestamp("created_at");
				int budgetId_MySQL = resultMySQL.getInt("budget_id");
				int recurringId_MySQL = resultMySQL.getInt("recurring_id");

				String amount_M = String.valueOf( (int)amount_MySQL );
				String budgetId_M = String.valueOf( (int)budgetId_MySQL );
				String recurringId_M = String.valueOf( (int)recurringId_MySQL );
				String auto_M = String.valueOf(auto_MySQL); 
				
				int transactionHashM = name_MySQL.hashCode() + amount_M.hashCode() + remark_MySQL.hashCode() + auto_M.hashCode() + 
						budgetId_M.hashCode() + recurringId_M.hashCode(); 
				
				//transactionsMapMySQL.put(id_MySQL, transactionHashM); 
				
				if(! (transactionsMapPostgres.get((Integer)id_MySQL)).equals((Integer)transactionHashM) ){
					LOGGER.debug("transactions hash inconsistency: expected '"+ transactionsMapPostgres.get(id_MySQL) +"' but received '"+ transactionHashM +"'");

					inconsistencies++;
				}

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
