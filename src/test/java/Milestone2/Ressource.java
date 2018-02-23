package Milestone2;

import java.util.List;

import javax.ws.rs.core.Response;

import io.budgetapp.modal.IdentityResponse;

public interface Ressource {
	
	public String post(String path, Object entity); 
	
	public String put(String path, Object entity);
	
	public String delete(String path);
	
	public String get(String path);
	
	public String getLocation();

}
