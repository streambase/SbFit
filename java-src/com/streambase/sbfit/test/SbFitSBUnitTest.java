package com.streambase.sbfit.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.streambase.sb.Tuple;
import com.streambase.sb.unittest.CSVTupleMaker;
import com.streambase.sb.unittest.Dequeuer;
import com.streambase.sb.unittest.Expecter;
import com.streambase.sb.unittest.FieldBasedTupleComparator;
import com.streambase.sb.unittest.SBServerManager;
import com.streambase.sb.unittest.ServerManagerFactory;

public class SbFitSBUnitTest {

	private static SBServerManager server;

	@BeforeClass
	public static void setupServer() throws Exception {
		// create a StreamBase server and load applications once for all tests in this class
		server = ServerManagerFactory.getEmbeddedServer();
		server.startServer();
		server.loadApp("SbFit.sbapp");
	}

	@AfterClass
	public static void stopServer() throws Exception {
		if (server != null) {
			server.shutdownServer();
			server = null;
		}
	}

	@Before
	public void startContainers() throws Exception {
		// before each test, startup fresh container instances
		server.startContainers();
	}

	@Test
	public void expectStrings() throws Exception {
		String [] input = {"2", "4", "5"};

		server.getEnqueuer("in").enqueue(CSVTupleMaker.MAKER, input);

		String [] output = {
				"2,3.14159,Irving,null",
				"4,4.14159,Irving,null",
				"5,3.14159,Irving,null",
		};
		
		FieldBasedTupleComparator comp = new FieldBasedTupleComparator(FieldBasedTupleComparator.DEFAULT_COMPARATOR, "value", "name", "pi");
		Expecter expecter = new Expecter(server.getDequeuer("out"), comp);
		expecter.expect(CSVTupleMaker.MAKER, output);
	}

	@Test
	public void expectTuples() throws Exception {
		String [] input = {"2", "4", "5"};

		server.getEnqueuer("in").enqueue(CSVTupleMaker.MAKER, input);

		String [] output = {
				"2,3.14159,Irving,null",
				"4,4.14159,Irving,null",
				"5,3.14159,Irving,null",
		};
		
		Dequeuer d = server.getDequeuer("out");
		List<Tuple> tuples = new ArrayList<Tuple>();
		
		for(String row : output) {
			Tuple t = CSVTupleMaker.MAKER.createTuple(d.getSchema(), row);
			
			tuples.add(t);
		}
		
		
		FieldBasedTupleComparator comp = new FieldBasedTupleComparator(FieldBasedTupleComparator.DEFAULT_COMPARATOR, "value", "name", "pi");
		Expecter expecter = new Expecter(d, comp);
		expecter.expect(tuples);
	}

	@After
	public void stopContainers() throws Exception {
		// after each test, dispose of the container instances
		server.stopContainers();
	}

}
