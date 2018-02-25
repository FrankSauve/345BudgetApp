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

import static org.hamcrest.CoreMatchers.is;

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
		long budgetId = mockedResourceIT.identityResponse(ResourceIT.post(path, addBudget)).getId();
		
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
		when(ResourceIT.put(path + budgetId, updateBudgetForm).readEntity(Budget.class)).thenReturn(updateBudget);
		
		Budget updatedBudgetEntity = ResourceIT.put(path + budgetId, updateBudgetForm).readEntity(Budget.class);
		
		//Assertions
		assertNotNull(mockedCreatedResponse.getLocation());
		assertEquals("Test", updatedBudgetEntity.getName());
		assertEquals(100, updatedBudgetEntity.getProjected(), 0.000);
	
	}	
	/**
	 * Created by @LeCleric 
	 * @throws URISyntaxException 
	 */
	@Test
	public void shouldAbleFindValidBudget() throws URISyntaxException {
		
		//Values needed for testing are initialized
		String name = "Name";
		long categoryId = 10;
		String path = "/api/budgets/10";
		URI testURI = new URI("http://localhost:9999/api/budgets/10");
		
		//Mock static methods of ResourceIT
		PowerMockito.mockStatic(ResourceIT.class);
		//Mock instance of ResourceIT
		ResourceIT mockedResourceIT = mock(ResourceIT.class);
		
		//Create addBudgetForm
        AddBudgetForm addBudget = new AddBudgetForm();
		addBudget.setName(name);
		addBudget.setCategoryId(categoryId);

		//Create mocked HTTP responses
		Response mockedPostResponse = mock(Response.class);
		Response mockedGetResponse = mock(Response.class);
		
		//Stub POST, get & getLocation calls
		when(ResourceIT.post(path, addBudget)).thenReturn(mockedPostResponse);
		when(mockedPostResponse.getLocation()).thenReturn(testURI);
		when(mockedResourceIT.get(path)).thenReturn(mockedGetResponse);
		
        // then create a newResponse Response calling the previously stubbed methods
        Response newResponse = mockedResourceIT.get(ResourceIT.post(path, addBudget).getLocation().getPath());
        
        //Stub the getStatus call (having the test work with mockedGetResponse proves that newResponse contains mockedGetResponse
        when(mockedGetResponse.getStatus()).thenReturn(200);
        
        //this is the equivalent of the assertOk method in ResourceIT
        assertThat(newResponse.getStatus(), is(200));
	}
}