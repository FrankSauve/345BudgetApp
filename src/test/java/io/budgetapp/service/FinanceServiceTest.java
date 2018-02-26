package io.budgetapp.service;


import io.budgetapp.model.Budget;
import io.budgetapp.model.Transaction;
import io.budgetapp.model.User;
import io.budgetapp.model.form.budget.AddBudgetForm;
import io.budgetapp.model.form.budget.UpdateBudgetForm;
import io.budgetapp.resource.BudgetResource;
import io.budgetapp.service.FinanceService;
import junit.framework.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.List;

import static org.mockito.Mockito.*;

/* Written by Costa Papadakos,  @Cotso */

public class FinanceServiceTest {

	//Test variables
	List<Budget> testList = null;
	List<Transaction> testTransac;
	List<String> testListString;
	
	int testMonth = 11;
	int testYear = 2012;
	long testId = 1;
	String testString = "q";
	
	//Mock Instances
	User mockUser = mock(User.class);
	Budget mockBudget = mock(Budget.class);
	FinanceService mockService = mock(FinanceService.class);
	AddBudgetForm mockForm = mock(AddBudgetForm.class);
	UpdateBudgetForm mockUpdate = mock(UpdateBudgetForm.class);
	
	//Real Instance of testing Class
	BudgetResource budget = new BudgetResource(mockService);

	@Test
	public void testCreated() {
		Assert.assertNotNull(budget);
	}
	
	@Test
	public void testBudgetByUser() {
		List<Budget> value;
		when(mockService.findBudgetsByUser(mockUser)).thenReturn(testList);
		value = budget.getBudgets(mockUser);
		verify(mockService).findBudgetsByUser(mockUser);
		Assert.assertEquals(testList, value);
	}
	@Test
	public void testPath() {
		Assert.assertEquals("/api/budgets", budget.getPath());
	}
	
	@Test
	public void testFindtransactions() {
		List<Transaction> value;
		when(mockService.findTransactionsByBudget(mockUser, testId)).thenReturn(testTransac);
		value = budget.findTransactions(mockUser, testId);
		verify(mockService).findTransactionsByBudget(mockUser, testId);
		Assert.assertEquals(testTransac, value);
	}
	
	@Test
	public void testFindSuggestion() {
		List<String> value;
		when(mockService.findBudgetSuggestions(mockUser, testString)).thenReturn(testListString);
		value = budget.findSuggestion(mockUser, testString);
		verify(mockService).findBudgetSuggestions(mockUser, testString);
		Assert.assertEquals(testListString, value);
	}
	
	@Test
	public void testFindById() {
		Budget testBudget;
		when(mockService.findBudgetById(mockUser, testId)).thenReturn(mockBudget);
		testBudget = budget.findById(mockUser, testId);
		verify(mockService).findBudgetById(mockUser, testId);
		Assert.assertEquals(mockBudget, testBudget);
	}
	
	@Test
	public void testGetBudgets() {
		List<Budget> value;
		when(mockService.findBudgetByUser(mockUser, testMonth, testYear)).thenReturn(testList);
		value = budget.getBudgets(mockUser, testMonth, testYear);
		verify(mockService).findBudgetByUser(mockUser, testMonth, testYear);
		Assert.assertEquals(testList, value);
	}
	
	@Test
	public void testAdd() {
		Response localResponse;
		when(mockService.addBudget(mockUser, mockForm)).thenReturn(mockBudget);
		localResponse = budget.add(mockUser, mockForm);
		verify(mockService).addBudget(mockUser, mockForm);
		Assert.assertNotNull(localResponse);
	}
	
	@Test
	public void testUpdate() {
		Response localResponse;
		when(mockService.updateBudget(mockUser, mockUpdate)).thenReturn(mockBudget);
		localResponse = budget.update(mockUser, testId, mockUpdate);
		verify(mockService).updateBudget(mockUser, mockUpdate);
		Assert.assertNotNull(localResponse);
	}
	
	@Test
	public void testDelete() {
		Response localResponse;
		localResponse = budget.delete(mockUser, testId);
		verify(mockService).deleteBudget(mockUser, testId);
		Assert.assertNotNull(localResponse);
	}
}
