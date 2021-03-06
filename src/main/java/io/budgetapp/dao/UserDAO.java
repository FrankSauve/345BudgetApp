package io.budgetapp.dao;

import io.budgetapp.application.NotFoundException;
import io.budgetapp.checker.ConsistencyChecker;
import io.budgetapp.database.MySqlConnector;
import io.budgetapp.database.PostgresConnector;
import io.budgetapp.model.User;
import io.budgetapp.model.form.SignUpForm;
import io.dropwizard.hibernate.AbstractDAO;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

/**
 *
 */
public class UserDAO extends AbstractDAO<User> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserDAO.class);

    public UserDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public User findById(Long userId) {
        User user = get(userId);
        if(user == null) {
            throw new NotFoundException();
        }
        return user;
    }

    public User add(SignUpForm signUp) {
        LOGGER.debug("Add new user {}", signUp);
        User user = new User();
        user.setUsername(signUp.getUsername());
        user.setPassword(signUp.getPassword());
        
        
        //***BEGIN Shadow write to mysql***
        if(MySqlConnector.getInstance().isUseMySql()){
        	String query = " INSERT INTO users (username, password, created_at)"
    				+ " VALUES (?, ?, ?)";
            PreparedStatement preparedStmt;
    		try {
    			preparedStmt = MySqlConnector.getInstance().getMySqlConnection().prepareStatement(query);
    			preparedStmt.setString(1, signUp.getUsername());
    			preparedStmt.setString(2, signUp.getPassword());
    			preparedStmt.setTimestamp(3,  new Timestamp(new java.util.Date().getTime()) );
    			try {
    				preparedStmt.execute();
    				//Check consistency
    				ConsistencyChecker checker = new ConsistencyChecker(PostgresConnector.getInstance().getPostgresConnection(), MySqlConnector.getInstance().getMySqlConnection());
    				checker.checkUsers();
    				if(checker.getNumInconsistencies() > 100) {
    					user = persist(user); //Call the old storage too
    				}
    			}
    			catch (SQLIntegrityConstraintViolationException  e) {
    				e.printStackTrace();
    			}
    		} catch (SQLException e) {
    			e.printStackTrace();
    		}
    		//***END Shadow write to mysql
        }
        
		
        return user;
    }

    public void update(User user) {
        LOGGER.debug("Update user {}", user);
        persist(user);
    }

    public Optional<User> findByUsername(String username) {
        Criteria criteria = criteria();
        criteria.add(Restrictions.eq("username", username).ignoreCase());
        List<User> users = list(criteria);
        if(users.size() == 1) {
            return Optional.of(users.get(0));
        } else {
            return Optional.empty();
        }
    }
}
