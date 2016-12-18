package com.epam.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.hamcrest.core.AnyOf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.stubbing.answers.DoesNothing;
import org.mockito.runners.MockitoJUnitRunner;

import com.epam.concurrency.task.AccidentDataEnricher;
import com.epam.concurrency.task.AccidentDataProcessor;
import com.epam.concurrency.task.AccidentDataReader;
import com.epam.concurrency.task.AccidentDataWriter;
import com.epam.concurrency.task.RoadAccidentDetails;
import com.epam.data.RoadAccident;
import com.epam.data.RoadAccidentBuilder;


@RunWith(MockitoJUnitRunner.class)
public class AccidentDataProcessorMockTest {
	
	private static List<String> fileQueue;
	private static final String FILE_PATH_2 = "src/main/resources/DfTRoadSafety_Accidents_2011.csv";
	@Mock
	private AccidentDataEnricher accidentDataEnricher;
	
	@InjectMocks
	private AccidentDataProcessor accidentDataProcessor;
	
	@Before
	public void setup() throws InterruptedException {
	MockitoAnnotations.initMocks(this);
	fileQueue = new ArrayList<>();
	accidentDataProcessor.init();
	
	}

	@Test
	public void shouldSetupProperly() {
		MatcherAssert.assertThat(accidentDataProcessor, Matchers.notNullValue());
		MatcherAssert.assertThat(accidentDataEnricher, Matchers.notNullValue());
		}
	@Test
	public void testWriter() throws InterruptedException
	{

		Mockito.doNothing().when(accidentDataEnricher).enrichPoliceForceContactAsynchronously(Mockito.anyObject());
		accidentDataProcessor.process();
		Mockito.verify(accidentDataEnricher,Mockito.times(1)).enrichPoliceForceContactAsynchronously(Mockito.anyObject());
	}
	
	@After
	public void teardown() {
		accidentDataProcessor = null;
		}
	 

	

}
