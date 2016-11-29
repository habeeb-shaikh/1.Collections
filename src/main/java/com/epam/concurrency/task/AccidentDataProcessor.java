package com.epam.concurrency.task;

import com.epam.data.RoadAccident;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Tanmoy on 6/17/2016.
 */
public class AccidentDataProcessor {

	private static final String FILE_PATH_1 = "src/main/resources/DfTRoadSafety_Accidents_2010.csv";
	private static final String FILE_PATH_2 = "src/main/resources/DfTRoadSafety_Accidents_2011.csv";
	private static final String FILE_PATH_3 = "src/main/resources/DfTRoadSafety_Accidents_2012.csv";
	private static final String FILE_PATH_4 = "src/main/resources/DfTRoadSafety_Accidents_2013.csv";

	private static final String OUTPUT_FILE_PATH = "target/DfTRoadSafety_Accidents_consolidated.csv";

	private static final int DATA_PROCESSING_BATCH_SIZE = 10000;

	private AccidentDataReader accidentDataReader = new AccidentDataReader();
	private AccidentDataEnricher accidentDataEnricher = new AccidentDataEnricher();
	private AccidentDataWriter accidentDataWriter = new AccidentDataWriter();

	private List<String> fileQueue = new ArrayList<String>();

	private Logger log = LoggerFactory.getLogger(AccidentDataProcessor.class);

	BlockingQueue<List<RoadAccident>> roadAccidentblockingQueue = new ArrayBlockingQueue<>(3);
	BlockingQueue<List<RoadAccidentDetails>> roadAccidentDtlsblockingQueue = new ArrayBlockingQueue<>(3);

	public void init() {
		fileQueue.add(FILE_PATH_1);
		fileQueue.add(FILE_PATH_2);
		fileQueue.add(FILE_PATH_3);
		fileQueue.add(FILE_PATH_4);

		accidentDataWriter.init(OUTPUT_FILE_PATH);
	}

	public void process() throws InterruptedException {
		for (String accidentDataFile : fileQueue) {
			log.info("Starting to process {} file ", accidentDataFile);
			accidentDataReader.init(DATA_PROCESSING_BATCH_SIZE, accidentDataFile);
			processFile();
		}
	}

	private void processFile() throws InterruptedException {
		Thread reader =  new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					reader();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		Thread processor =  new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					processor();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		Thread writer =  new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					writer();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
		
		reader.start();
		processor.start();
		writer.start();
		
		reader.join();
		processor.join();
		writer.join();
	}

	private void reader() throws InterruptedException {
		int batchCount = 1;
		while (!accidentDataReader.hasFinished()) {
			List<RoadAccident> roadAccidents = accidentDataReader.getNextBatch(); // Read
			roadAccidentblockingQueue.put(roadAccidents);
			log.info("Read [{}] records in batch [{}]", roadAccidents.size(), batchCount++);
		}
	}

	private void processor() throws InterruptedException {
		while (true) {
			List<RoadAccidentDetails> roadAccidentDetailsList = accidentDataEnricher
					.enrichRoadAccidentData(roadAccidentblockingQueue.take()); // Process
			roadAccidentDtlsblockingQueue.put(roadAccidentDetailsList);
			log.info("Enriched records");

		}
	}

	private void writer() throws InterruptedException {
		while (true) {
			accidentDataWriter.writeAccidentData(roadAccidentDtlsblockingQueue.take()); // Write
			log.info("Written records");
		}

	}

	public static void main(String[] args) throws InterruptedException {
		AccidentDataProcessor dataProcessor = new AccidentDataProcessor();
		long start = System.currentTimeMillis();
		dataProcessor.init();
		dataProcessor.process();
		long end = System.currentTimeMillis();
		System.out.println("Process finished in s : " + (end - start) / 1000);
	}

}
