package io.budgetapp.resource;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.Response;

import io.budgetapp.model.Category;
import io.budgetapp.model.CategoryType;


@RunWith(PowerMockRunner.class)
@PrepareForTest(ResourceIT.class)

public class MockCategoryResourceIT {

	/**
	 * Created by @Sarbeng
	 * @throws URISyntaxException 
	 */
	@Test
	public void shouldAbleFindValidCategory() throws URISyntaxException{
		String path = "/api/categories/5";
		URI testURI = new URI("http://localhost:9999/api/categories/5");
		//Mock static methods of ResourceIT
		PowerMockito.mockStatic(ResourceIT.class);
		//Mock instance of ResourceIT
		ResourceIT mockedResourceIT = mock(ResourceIT.class);
		String name = "John Smith";
		CategoryType categoryType = CategoryType.EXPENDITURE;
		long originalCategoryId = 5;
		Category category = new Category();
						
		//create a category
		category.setName(name);
		category.setType(categoryType);
		category.setId(originalCategoryId);
		assertEquals(name, category.getName());
		assertEquals(categoryType, category.getType());
		assertEquals(originalCategoryId, category.getId(),0);
		
		//Create mocked HTTP responses
		Response mockedPostResponse = mock(Response.class);
		Response mockedGetResponse = mock(Response.class);
				
		//Stub post, get & getLocation calls
		when(ResourceIT.post(path, category)).thenReturn(mockedPostResponse);
		when(mockedPostResponse.getLocation()).thenReturn(testURI);
		when(mockedResourceIT.get(path)).thenReturn(mockedGetResponse);
		
        // a new Response calling the previously stubbed methods
        Response newResponse = mockedResourceIT.get(ResourceIT.post(path, category).getLocation().getPath());
        
        //Stub the getStatus call
        when(mockedGetResponse.getStatus()).thenReturn(200);
        
        //this is the equivalent of the assertOk method in ResourceIT
        assertThat(newResponse.getStatus(), is(200));		
		
		
	}

}
