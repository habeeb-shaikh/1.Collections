package com.epam.concurrency.task;

import com.epam.data.RoadAccident;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by Tanmoy on 6/16/2016.
 */
public class AccidentDataEnricher {

    private PoliceForceExternalDataService policeForceService = new PoliceForceExternalDataService();

    public List<RoadAccidentDetails> enrichRoadAccidentData(List<RoadAccident> roadAccidents){
        List<RoadAccidentDetails> roadAccidentDetailsList = new ArrayList<>(roadAccidents.size());
        for(RoadAccident roadAccident : roadAccidents){
            roadAccidentDetailsList.add(enrichRoadAccidentDataItem(roadAccident));
        }
        Util.sleepToSimulateDataHeavyProcessing();
        return roadAccidentDetailsList;
    }

    public RoadAccidentDetails enrichRoadAccidentDataItem(RoadAccident roadAccident){
        RoadAccidentDetails roadAccidentDetails = new RoadAccidentDetails(roadAccident);
        enrichPoliceForceContactSynchronously(roadAccidentDetails);
//        enrichPoliceForceContactAsynchronously(roadAccidentDetails);
        /**
         * above call might get blocked causing the application to get stuck
         *
         * solve this problem by accessing the the PoliceForceExternalDataService asynchronously
         * with a timeout of 30 S
         *
         * use method "enrichPoliceForceContactAsynchronously" instead
         */
        return  roadAccidentDetails;
    }

    public void enrichPoliceForceContactSynchronously(RoadAccidentDetails roadAccidentDetails){
        String policeForceContact = policeForceService.getContactNoWithoutDelay(roadAccidentDetails.getPoliceForce());
        roadAccidentDetails.setPoliceForceContact(policeForceContact);
    }

    public void enrichPoliceForceContactAsynchronously(RoadAccidentDetails roadAccidentDetails){
        //use policeForceService.getContactNoWithDelay
    	
    	String policeForceContact = policeForceService.getContactNoWithDelay(roadAccidentDetails.getPoliceForce());
        roadAccidentDetails.setPoliceForceContact(policeForceContact);
        
        ExecutorService executor = Executors.newCachedThreadPool();
        Callable<Object> task = new Callable<Object>() {
           public Object call() {
        	   String policeForceContact = policeForceService.getContactNoWithDelay(roadAccidentDetails.getPoliceForce());
   	           roadAccidentDetails.setPoliceForceContact(policeForceContact);
   	           return null;
           }
        };
        Future<Object> future = executor.submit(task);
        try {
           Object result = future.get(30, TimeUnit.SECONDS); 
        } catch (TimeoutException ex) {
           // handle the timeout
        } catch (InterruptedException e) {
           // handle the interrupts
        } catch (ExecutionException e) {
           // handle other exceptions
        } finally {
           future.cancel(true); // may or may not desire this
        }
    }
}
