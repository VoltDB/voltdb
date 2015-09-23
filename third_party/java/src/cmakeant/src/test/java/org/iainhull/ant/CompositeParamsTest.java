package org.iainhull.ant;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class CompositeParamsTest {
	Params first;
	Params second;
	CompositeParams composite;
	
	
	@Before
	public void setUp() throws Exception {
		first = new SimpleParams();
		second = new SimpleParams();
		composite = new CompositeParams(first, second);
	}

	@Test
	public void testIsCleanfirstSet() {
		assertFalse(first.isCleanfirstSet());
		assertFalse(second.isCleanfirstSet());
		assertFalse(composite.isCleanfirstSet());
		
		first.setCleanfirst(true);
		assertTrue(composite.isCleanfirstSet());
		assertTrue(composite.isCleanfirst());
		assertTrue(first.isCleanfirstSet());
		assertFalse(second.isCleanfirstSet());

		second.setCleanfirst(false);
		assertTrue(composite.isCleanfirstSet());
		assertFalse(composite.isCleanfirst());
		assertTrue(first.isCleanfirstSet());
		assertTrue(second.isCleanfirstSet());
	}

}
