package com.epam.processor;

import com.epam.concurrency.task.AccidentDataProcessor;
import com.epam.data.AccidentsDataLoader;
import com.epam.data.RoadAccident;
import com.google.common.collect.Multimap;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

/**
 * Created by Tkachi on 2016/4/4.
 */
public class AccidentDataProcessorTest {

    private static final String ACCIDENTS_CSV = "src/main/resources/DfTRoadSafety_Accidents_2009.csv";

    private static AccidentDataProcessor dataProcessor;

    @BeforeClass
    public static void loadData(){
        dataProcessor = new AccidentDataProcessor();
        dataProcessor.init();
    }

    @Test
    public void should_read_file_path(){
    	String fileName="src/main/resources/DfTRoadSafety_Accidents_2010.csv";
    	List<String> fileQueue =  dataProcessor.getFileQueue();
    	assertThat(fileQueue.size(),equalTo(4));
    	assertThat(fileQueue, hasItems(fileName));
        assertNotNull(fileQueue);
    }

    @Test
    public void should_verify_file_created(){
        String OUTPUT_FILE_PATH = "target/DfTRoadSafety_Accidents_consolidated.csv";
        File outputFile = new File(OUTPUT_FILE_PATH);
        assertTrue(outputFile.exists());
    }
    @Test
    public void data_process_batch_size(){
    	int size= dataProcessor.getDataProcessBatchSize();
    	assertThat(size, equalTo(10000));
    }
     

}