package io.budgetapp.resource;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;

import javax.ws.rs.core.Response;

import io.budgetapp.modal.IdentityResponse;
import io.budgetapp.model.Budget;
import io.budgetapp.model.form.budget.AddBudgetForm;
import io.budgetapp.model.form.budget.UpdateBudgetForm;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ResourceIT.class)
public class MockBudgetResourceIT {

	/**
	 * Created by @franksauve
	 * @throws URISyntaxException 
	 */
	@Test
	public void shouldBeAbleUpdateBudget() throws URISyntaxException {
		
		String path = "/api/budgets";
		
		//Mock static methods of ResourceIT
		PowerMockito.mockStatic(ResourceIT.class);
		//Mock instance of ResourceIT
		ResourceIT mockedResourceIT = mock(ResourceIT.class);
		
		//Add budget form
		AddBudgetForm addBudget = new AddBudgetForm();
		String originalName = "Original Name";
		long originalCategoryId = 10;
		addBudget.setName(originalName);
		addBudget.setCategoryId(originalCategoryId);

		//Mock create response
		Response mockedCreatedResponse = mock(Response.class);
		IdentityResponse mockedCreatedIdResponse = mock(IdentityResponse.class);
		
		//Stub POST request
		when(ResourceIT.post(path, addBudget)).thenReturn(mockedCreatedResponse);
		
		//Stub getId
		when(mockedResourceIT.identityResponse(mockedCreatedResponse)).thenReturn(mockedCreatedIdResponse);
		when(mockedResourceIT.identityResponse(mockedCreatedResponse).getId()).thenReturn((long) 10);
		long budgetId = mockedResourceIT.identityResponse(mockedCreatedResponse).getId();
		
		//Stub getLocation
		when(mockedCreatedResponse.getLocation()).thenReturn(new URI("http://localhost:9999/api/budgets/10"));
		
		//Update budget form
		UpdateBudgetForm updateBudgetForm = new UpdateBudgetForm();
		updateBudgetForm.setId(budgetId);
		updateBudgetForm.setName("Test");
		updateBudgetForm.setProjected(100);
		
		//Mock update response
		Response mockedUpdateResponse = mock(Response.class);
		
		//Stub PUT request
		when(ResourceIT.put(path + budgetId, updateBudgetForm)).thenReturn(mockedUpdateResponse);
		
		//Budget entity that should be returned
		Budget updateBudget = new Budget();
		updateBudget.setId(budgetId);
		updateBudget.setName("Test");
		updateBudget.setProjected(100);
		
		//Stub readEntity
		when(mockedUpdateResponse.readEntity(Budget.class)).thenReturn(updateBudget);
		Budget updatedBudgetEntity = mockedUpdateResponse.readEntity(Budget.class);
		
		//Assertions
		assertNotNull(mockedCreatedResponse.getLocation());
		assertEquals("Test", updatedBudgetEntity.getName());
		assertEquals(100, updatedBudgetEntity.getProjected(), 0.000);
	
	}	
}