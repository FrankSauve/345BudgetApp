package Milestone2;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;

import io.budgetapp.model.Category;
import io.budgetapp.model.CategoryType;



public class MockCategoryResourceITTest {

	@Test
	public void shouldAbleFindValidCategory() {
		String name = "John Smith";
		CategoryType category = CategoryType.EXPENDITURE;
		String path = "/api/categories";
		String postValue = "InboundJaxrsResponse{context=ClientResponse{method=POST, uri=http://localhost:9999/api/categories, status=201, reason=Created}}";
		String getValue = "InboundJaxrsResponse{context=ClientResponse{method=GET, uri=http://localhost:9999/api/categories, status=201, reason=Created}}";
		 
		
		Category mockedCategory = mock(Category.class);
		Ressource mockedRessource = mock(Ressource.class);
		String mocked2Ressource;
		
		//name can be any String
		mockedCategory.setName(name);
		when(mockedCategory.getName()).thenReturn(name);
		verify(mockedCategory).setName(name);
		assertEquals(name, mockedCategory.getName());
		
		//Can be either "Income" or "Expenditure"
		mockedCategory.setType(category);
		when(mockedCategory.getType()).thenReturn(category);
		verify(mockedCategory).setType(category);
		assertEquals(category, mockedCategory.getType());
		
		//create mocked ressource which returns a post String JSON value
		when(mockedRessource.post(path, mockedCategory)).thenReturn(postValue);
		
		//create mocked ressource which returns a get String JSON value
		when(mockedRessource.getLocation()).thenReturn(getValue);
		
		//acts as a second response, but in the form of a String
		mocked2Ressource = mockedRessource.getLocation();
		
		verify(mockedRessource).getLocation();
		assert(mockedRessource.post(path, mockedCategory).contains("201"));
		assert(mocked2Ressource.contains("201"));
	
	}	
}



