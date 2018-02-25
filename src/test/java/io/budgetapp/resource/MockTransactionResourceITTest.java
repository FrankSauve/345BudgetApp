package io.budgetapp.resource;

import static org.hamcrest.CoreMatchers.is;
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
import io.budgetapp.model.Transaction;


@RunWith(PowerMockRunner.class)
@PrepareForTest(ResourceIT.class)
public class MockTransactionResourceITTest {
	
	/**
	 * 
	 * @author RaphaelleG
	 * @throws URISyntaxException
	 *
	 */
	@Test
	public void shouldNotAbleDeleteInvalidTransaction() throws URISyntaxException{
		
		String path = "/api/transactions/"; 
		
		//Mock static methods of ResourceIT
		PowerMockito.mockStatic(ResourceIT.class);
		//Mock instance of ResourceIT
		ResourceIT mockedResourceIT = mock(ResourceIT.class);
		
		//Add transaction id
		long transactionId = Long.MAX_VALUE; 
		
		//Mock delete response 
		Response mockedDeleteResponse = mock(Response.class);
		
		//Stub PUT request 
		when(ResourceIT.delete(path + transactionId)).thenReturn(mockedDeleteResponse); 
		
		//Assertion 
		assertThat(mockedDeleteResponse.getStatus(), is(Response.Status.NOT_FOUND.getStatusCode()));		
	}

}
