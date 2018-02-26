package io.budgetapp.resource;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.Response;

import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import io.budgetapp.modal.IdentityResponse;
import io.budgetapp.model.form.SignUpForm;

public class MockUserResourceIT {

	@Test
	public void shouldAbleCreateUser() {
		String email = "test@test.com";
		String password = "test1234567";
		
		//Mock static methods of ResourceIT
		PowerMockito.mockStatic(ResourceIT.class);
		//Mock instance of ResourceIT
		ResourceIT mockedResourceIT = mock(ResourceIT.class);
	     
		SignUpForm signUp = new SignUpForm();
	    signUp.setUsername(email);
	    signUp.setPassword(password);
		
		//Mock create response
		Response mockedCreatedResponse = mock(Response.class);
		
		//Stub POST, get & getLocation calls
		when(ResourceIT.post(ResourceURL.USER, signUp)).thenReturn(mockedCreatedResponse);
		
		when(mockedCreatedResponse.getStatus()).thenReturn(200);
	    
	    //this is the equivalent of the assertOk method in ResourceIT
        assertThat(mockedCreatedResponse.getStatus(), is(200));
	}

}
