package com.epam.processor;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import com.epam.concurrency.task.AccidentDataProcessor;
import com.epam.concurrency.task.RoadAccidentCsvParser;
import com.epam.data.RoadAccident;

public class AccidentDataIntegrationTest {

	private String OUTPUT_FILE_PATH = "target/DfTRoadSafety_Accidents_consolidated.csv";
	private RoadAccidentCsvParser roadAccidentParser = new RoadAccidentCsvParser();
	private Iterator<CSVRecord> recordIterator;

	private static AccidentDataProcessor dataProcessor;

	@BeforeClass
	public static void loadData() throws InterruptedException {
		dataProcessor = new AccidentDataProcessor();
		dataProcessor.init();
//		dataProcessor.process();
	}

	@Test
	public void verify_File_Created() {
		File outputFile = new File(OUTPUT_FILE_PATH);
		assertTrue(outputFile.exists());

	}

	@Test
	public void verify_Records() {
		this.prepareIterator();
		assertTrue("Record Not Founct",getRecord());

	}

	private void prepareIterator() {
		try {
			Reader reader = new FileReader(OUTPUT_FILE_PATH);
			recordIterator = new CSVParser(reader, CSVFormat.EXCEL.withHeader()).iterator();
		} catch (Exception e) {
			throw new RuntimeException("Failed to prepare file iterator for  file : " + OUTPUT_FILE_PATH, e);
		}
	}

	public boolean getRecord() {
		int recordCountInCurrBatch = 0;
		RoadAccident roadAccidentItem = null;
		while (recordCountInCurrBatch < 10000 && recordIterator.hasNext()) {
				return true;
		}
		return false;
	}
}
