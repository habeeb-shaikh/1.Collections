package com.epam.processor;

import com.epam.data.RoadAccident;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is to be completed by mentees
 */
public class DataProcessor {

	private final List<RoadAccident> roadAccidentList;

	public DataProcessor(List<RoadAccident> roadAccidentList) {
		this.roadAccidentList = roadAccidentList;
	}

	// First try to solve task using java 7 style for processing collections

	/**
	 * Return road accident with matching index
	 * 
	 * @param index
	 * @return
	 */
	public RoadAccident getAccidentByIndex7(String index) {
		RoadAccident roadAccidentByIndex = null;
		if (!"".equalsIgnoreCase(index)) {
			for (RoadAccident roadAccident : roadAccidentList) {
				if (index.equalsIgnoreCase("" + roadAccident.getAccidentId())) {
					roadAccidentByIndex = roadAccident;
					break;
				}
			}
		}
		return roadAccidentByIndex;
	}

	/**
	 * filter list by longtitude and latitude values, including boundaries
	 * 
	 * @param minLongitude
	 * @param maxLongitude
	 * @param minLatitude
	 * @param maxLatitude
	 * @return
	 */
	public Collection<RoadAccident> getAccidentsByLocation7(float minLongitude, float maxLongitude, float minLatitude,
			float maxLatitude) {
		Collection<RoadAccident> accidentsByLocation = new ArrayList<>();
		for (RoadAccident roadAccident : roadAccidentList) {
			if (isInSpecifiedRange(minLongitude, maxLongitude, minLatitude, maxLatitude, roadAccident)) {
				accidentsByLocation.add(roadAccident);
			}
			roadAccident = null;
		}
		return accidentsByLocation;
	}

	/**
	 * count incidents by road surface conditions ex: wet -> 2 dry -> 5
	 * 
	 * @return
	 */
	public Map<String, Integer> getCountByRoadSurfaceCondition7() {
		Map<String, Integer> countByRoadSurfaceConditionMap = new HashMap<>();
		Integer count = 1;
		String roadSurfaceMapKey;
		for (RoadAccident roadAccident : roadAccidentList) {
			roadSurfaceMapKey = roadAccident.getRoadSurfaceConditions();
			if (countByRoadSurfaceConditionMap.containsKey(roadSurfaceMapKey)) {
				count = countByRoadSurfaceConditionMap.get(roadSurfaceMapKey);
				count = count + 1;
				countByRoadSurfaceConditionMap.put(roadSurfaceMapKey, count);
			} else {
				countByRoadSurfaceConditionMap.put(roadSurfaceMapKey, count);
			}
			roadSurfaceMapKey = null;
		}
		return countByRoadSurfaceConditionMap;
	}

	/**
	 * find the weather conditions which caused the top 3 number of incidents as
	 * example if there were 10 accidence in rain, 5 in snow, 6 in sunny and 1
	 * in foggy, then your result list should contain {rain, sunny, snow} - top
	 * three in decreasing order
	 * 
	 * @return
	 */
	public List<String> getTopThreeWeatherCondition7() {

		List<String> top3WeatherList = new ArrayList<String>();
		Map<String, Integer> weatherCondMap = getWeatherCondition7();
		List<Integer> weatherCondList = new ArrayList<Integer>(weatherCondMap.values());
		Collections.sort(weatherCondList, Collections.reverseOrder());
		List<Integer> topThreeWeathersList = weatherCondList.subList(0, 3);
		Set<String> weatherCondSet = weatherCondMap.keySet();
		Integer count = 0;
		for (String weatherCond : weatherCondSet) {
			count = weatherCondMap.get(weatherCond);
			for (Integer topThreeWeather : topThreeWeathersList) {
				if (topThreeWeather == count) {
					top3WeatherList.add(weatherCond);
				}
			}
		}
		weatherCondList = null;
		weatherCondMap = null;
		topThreeWeathersList = null;
		weatherCondSet = null;
		return top3WeatherList;

	}

	/**
	 * return a multimap where key is a district authority and values are
	 * accident ids ex: authority1 -> id1, id2, id3 authority2 -> id4, id5
	 * 
	 * @return
	 */
	public Multimap<String, String> getAccidentIdsGroupedByAuthority7() {

		ListMultimap<String, String> accidentIdsGroupedByAuthorityMap = ArrayListMultimap.create();
		String roadSurfaceMapKey;
		String accidents;
		for (RoadAccident roadAccident : roadAccidentList) {
			roadSurfaceMapKey = (roadAccident.getDistrictAuthority());
			accidents = roadAccident.getAccidentId();
			accidentIdsGroupedByAuthorityMap.put(roadSurfaceMapKey, accidents);
			roadSurfaceMapKey = null;
			accidents = null;
		}
		return accidentIdsGroupedByAuthorityMap;
	}

	// Now let's do same tasks but now with streaming api

	public RoadAccident getAccidentByIndex(String index) {

		RoadAccident roadAccident;

		roadAccident = roadAccidentList.stream() // Convert to steam
				.filter(roadAccidentByIndex -> index.equals(roadAccidentByIndex.getAccidentId())).findAny()
				.orElse(null);
		return roadAccident;
	}

	/**
	 * filter list by longtitude and latitude fields
	 * 
	 * @param minLongitude
	 * @param maxLongitude
	 * @param minLatitude
	 * @param maxLatitude
	 * @return
	 */
	public Collection<RoadAccident> getAccidentsByLocation(float minLongitude, float maxLongitude, float minLatitude,
			float maxLatitude) {

		Collection<RoadAccident> accidentsByLocation = new ArrayList<>();

		accidentsByLocation = (Collection<RoadAccident>) roadAccidentList.stream()
				.filter(roadAccident -> minLongitude < roadAccident.getLongitude()
						&& maxLongitude > roadAccident.getLongitude())
				.filter(roadAccident -> minLatitude < roadAccident.getLatitude()
						&& maxLatitude > roadAccident.getLatitude())
				.collect(Collectors.toList());

		return accidentsByLocation;

	}

	/**
	 * find the weather conditions which caused max number of incidents
	 * 
	 * @return
	 */
	public List<String> getTopThreeWeatherCondition() {
		List<String> top3WeatherList = new ArrayList<String>();

		top3WeatherList = roadAccidentList.stream()
				.collect(Collectors.groupingBy(RoadAccident::getWeatherConditions, Collectors.counting())).entrySet()
				.stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(3)
				.map(RoadAccident -> RoadAccident.getKey()).collect(Collectors.toList());

		return top3WeatherList;
	}

	/**
	 * count incidents by road surface conditions
	 * 
	 * @return
	 */
	public Map<String, Long> getCountByRoadSurfaceCondition() {

		Map<String, Long> countByRoadSurfaceConditionMap = new HashMap<>();

		countByRoadSurfaceConditionMap = roadAccidentList.stream()
				.collect(Collectors.groupingBy(RoadAccident::getRoadSurfaceConditions, Collectors.counting()));

		return countByRoadSurfaceConditionMap;
	}

	/**
	 * To match streaming operations result, return type is a java collection
	 * instead of multimap
	 * 
	 * @return
	 */
	public Map<String, List<String>> getAccidentIdsGroupedByAuthority() {

		Map<String, List<String>> accidentIdsGroupedByAuthorityMap = new HashMap<>();

		accidentIdsGroupedByAuthorityMap = roadAccidentList.stream()
				.collect(Collectors.groupingBy(RoadAccident::getDistrictAuthority,
						Collectors.mapping(RoadAccident::getAccidentId, Collectors.toList())));
		return accidentIdsGroupedByAuthorityMap;
	}

	/**
	 * This method verifies range of location
	 * 
	 * @param minValue
	 * @param maxValue
	 * @param roadAccident
	 * @return true when in range other wise false
	 */
	private boolean isInSpecifiedRange(float minLongitude, float maxLongitude, float minLatitude, float maxLatitude,
			RoadAccident roadAccident) {
		boolean isInRange = false;
		if (minLongitude < roadAccident.getLongitude() && maxLongitude > roadAccident.getLongitude()
				&& minLatitude < roadAccident.getLatitude() && maxLatitude > roadAccident.getLatitude()) {
			isInRange = true;
		}
		return isInRange;
	}

	public Map<String, Integer> getWeatherCondition7() {
		Map<String, Integer> countByWeatherConditionMap = new HashMap<>();
		Integer count = 1;
		String weatherCondMapKey;
		for (RoadAccident roadAccident : roadAccidentList) {
			weatherCondMapKey = (roadAccident.getWeatherConditions());
			if (countByWeatherConditionMap.containsKey(weatherCondMapKey)) {
				count = countByWeatherConditionMap.get(weatherCondMapKey);
				count = count + 1;
				countByWeatherConditionMap.put(weatherCondMapKey, count);
				count = 0;
			} else {
				countByWeatherConditionMap.put(weatherCondMapKey, count);
			}
		}
		return countByWeatherConditionMap;
	}
}
