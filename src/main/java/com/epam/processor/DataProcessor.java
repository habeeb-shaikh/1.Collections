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
		RoadAccident _RoadAccidentByIndex = null;
		if (!"".equalsIgnoreCase(index)) {
			for (RoadAccident _RoadAccident : roadAccidentList) {
				if (index.equalsIgnoreCase("" + _RoadAccident.getAccidentId())) {
					_RoadAccidentByIndex = _RoadAccident;
					break;
				}
			}
		}
		return _RoadAccidentByIndex;
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
		Collection<RoadAccident> _AccidentsByLocation = new ArrayList<>();
		for (RoadAccident _RoadAccident : roadAccidentList) {
			if (isInSpecifiedRange(minLongitude, maxLongitude, _RoadAccident, true)
					&& isInSpecifiedRange(minLatitude, maxLatitude, _RoadAccident, false)) {
				_AccidentsByLocation.add(_RoadAccident);
			}
			_RoadAccident = null;
		}
		return _AccidentsByLocation;
	}

	/**
	 * count incidents by road surface conditions ex: wet -> 2 dry -> 5
	 * 
	 * @return
	 */
	public Map<String, Integer> getCountByRoadSurfaceCondition7() {
		Map<String, Integer> _CountByRoadSurfaceConditionMap = new HashMap<>();
		Integer _Count = (int) 1;
		String _RoadSurfaceMapKey;
		for (RoadAccident roadAccident : roadAccidentList) {
			_RoadSurfaceMapKey = roadAccident.getRoadSurfaceConditions();
			if (_CountByRoadSurfaceConditionMap.containsKey(_RoadSurfaceMapKey)) {
				_Count = _CountByRoadSurfaceConditionMap.get(_RoadSurfaceMapKey);
				_Count = _Count + 1;
				_CountByRoadSurfaceConditionMap.put(_RoadSurfaceMapKey, _Count);
			} else {
				_CountByRoadSurfaceConditionMap.put(_RoadSurfaceMapKey, _Count);
			}
			_RoadSurfaceMapKey = null;
		}
		return _CountByRoadSurfaceConditionMap;
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

		List<String> _Top3WeatherList = new ArrayList<String>();
		Map<String, Integer> _WeatherCondMap = getWeatherCondition7();
		List<Integer> _WeatherCondList = new ArrayList<Integer>(_WeatherCondMap.values());
		Collections.sort(_WeatherCondList, Collections.reverseOrder());
		List<Integer> _TopThreeWeathersList = _WeatherCondList.subList(0, 3);
		Set<String> _WeatherCondSet = _WeatherCondMap.keySet();
		Integer count = 0;
		for (String _WeatherCond : _WeatherCondSet) {
			count = _WeatherCondMap.get(_WeatherCond);
			for (Integer _TopThreeWeather : _TopThreeWeathersList) {
				if (_TopThreeWeather == count) {
					_Top3WeatherList.add(_WeatherCond);
				}
			}
		}
		_WeatherCondList = null;
		_WeatherCondMap = null;
		_TopThreeWeathersList = null;
		_WeatherCondSet = null;
		return _Top3WeatherList;

	}

	/**
	 * return a multimap where key is a district authority and values are
	 * accident ids ex: authority1 -> id1, id2, id3 authority2 -> id4, id5
	 * 
	 * @return
	 */
	public Multimap<String, String> getAccidentIdsGroupedByAuthority7() {

		ListMultimap<String, String> _AccidentIdsGroupedByAuthorityMap = ArrayListMultimap.create();
		String _RoadSurfaceMapKey;
		String _Accidents;
		for (RoadAccident roadAccident : roadAccidentList) {
			_RoadSurfaceMapKey = (roadAccident.getDistrictAuthority());
			_Accidents = roadAccident.getAccidentId();
			_AccidentIdsGroupedByAuthorityMap.put(_RoadSurfaceMapKey, _Accidents);
			_RoadSurfaceMapKey = null;
			_Accidents = null;
		}
		return _AccidentIdsGroupedByAuthorityMap;
	}

	// Now let's do same tasks but now with streaming api

	public RoadAccident getAccidentByIndex(String index) {

		RoadAccident _RoadAccident;

		_RoadAccident = roadAccidentList.stream() // Convert to steam
				.filter(_RoadAccidentByIndex -> index.equals(_RoadAccidentByIndex.getAccidentId())).findAny() 
				.orElse(null);
		return _RoadAccident;
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
		
		
		Collection<RoadAccident> _AccidentsByLocation = new ArrayList<>();
		
		_AccidentsByLocation = (Collection<RoadAccident>) roadAccidentList.stream()
		.filter( roadAccident -> minLongitude < roadAccident.getLongitude()  && maxLongitude > roadAccident.getLongitude())
		.filter( roadAccident -> minLatitude < roadAccident.getLatitude()  && maxLatitude > roadAccident.getLatitude())
		.collect(Collectors.toList());	
		
		return _AccidentsByLocation;
		
	}

	/**
	 * find the weather conditions which caused max number of incidents
	 * 
	 * @return
	 */
	public List<String> getTopThreeWeatherCondition() {
		List<String> _Top3WeatherList = new ArrayList<String>();
		
		_Top3WeatherList = roadAccidentList.stream()
		.collect(Collectors.groupingBy(RoadAccident::getWeatherConditions,Collectors.counting()))
		.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(3)
		.map(RoadAccident -> RoadAccident.getKey()).collect(Collectors.toList());
  
		
		return _Top3WeatherList;
	}

	/**
	 * count incidents by road surface conditions
	 * 
	 * @return
	 */
	public Map<String, Long> getCountByRoadSurfaceCondition() {

		Map<String, Long> _CountByRoadSurfaceConditionMap = new HashMap<>();
		
		_CountByRoadSurfaceConditionMap = roadAccidentList.stream()
				.collect(Collectors.groupingBy(RoadAccident::getRoadSurfaceConditions,Collectors.counting()));
		
		return   _CountByRoadSurfaceConditionMap;
	}

	/**
	 * To match streaming operations result, return type is a java collection
	 * instead of multimap
	 * 
	 * @return
	 */
	public Map<String, List<String>> getAccidentIdsGroupedByAuthority() {
		
		
		Map<String, List<String>> _AccidentIdsGroupedByAuthorityMap = new HashMap<>();
		
		_AccidentIdsGroupedByAuthorityMap = roadAccidentList.stream()
				.collect(Collectors.groupingBy(RoadAccident::getDistrictAuthority,
						Collectors.mapping(RoadAccident::getAccidentId,Collectors.toList())));
		return _AccidentIdsGroupedByAuthorityMap;
	}

	/**
	 * This method verifies range of location
	 * 
	 * @param _MinValue
	 * @param _MaxValue
	 * @param _RoadAccident
	 * @return true when in range other wise false
	 */
	private boolean isInSpecifiedRange(float _MinValue, float _MaxValue, RoadAccident _RoadAccident,
			boolean _Longitude) {
		boolean _IsInRange = false;
		if (_Longitude) {
			if (_MinValue < _RoadAccident.getLongitude() && _MaxValue > _RoadAccident.getLongitude()) {
				_IsInRange = true;
			}
		} else {
			if (_MinValue < _RoadAccident.getLatitude() && _MaxValue > _RoadAccident.getLatitude()) {
				_IsInRange = true;
			}
		}

		return _IsInRange;
	}

	public Map<String, Integer> getWeatherCondition7() {
		Map<String, Integer> _CountByWeatherConditionMap = new HashMap<>();
		Integer _Count = 1;
		String _WeatherCondMapKey;
		for (RoadAccident roadAccident : roadAccidentList) {
			_WeatherCondMapKey = (roadAccident.getWeatherConditions());
			if (_CountByWeatherConditionMap.containsKey(_WeatherCondMapKey)) {
				_Count = _CountByWeatherConditionMap.get(_WeatherCondMapKey);
				_Count = _Count + 1;
				_CountByWeatherConditionMap.put(_WeatherCondMapKey, _Count);
				_Count = 0;
			} else {
				_CountByWeatherConditionMap.put(_WeatherCondMapKey, _Count);
			}
		}
		return _CountByWeatherConditionMap;
	}
}
