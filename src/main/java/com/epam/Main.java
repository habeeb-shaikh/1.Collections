package com.epam;

import com.epam.data.AccidentsDataLoader;
import com.epam.data.RoadAccident;
import com.epam.processor.DataProcessor;
import com.epam.processor.DataProcessorTest;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tkachi on 2016/4/3.
 */
public class Main {

    private static final String ACCIDENTS_CSV = "src/main/resources/DfTRoadSafety_Accidents_2009.csv";


    public static void main(String[] args) throws IOException {
        AccidentsDataLoader accidentsDataLoader = new AccidentsDataLoader();
        List<RoadAccident> accidents = accidentsDataLoader.loadRoadAccidents(ACCIDENTS_CSV);
//        new DataProcessor(accidents).getAccidentsByLocation(-0.2f,-0.1f,51f,52f);
//        new DataProcessor(accidents).getCountByRoadSurfaceCondition();
//        new DataProcessor(accidents).getAccidentIdsGroupedByAuthority7();
//        new DataProcessor(accidents).getAccidentIdsGroupedByAuthority();
        new DataProcessor(accidents).getTopThreeWeatherCondition();


    }

}
