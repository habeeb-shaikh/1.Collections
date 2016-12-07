# RoadAccidentsMgmt

##Setting up environment

### Instal required software

1) Downloadand install JDK 8 http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html

2) Download and install Git bash https://git-scm.com/download/win 

3) Download and install Apache Maven http://maven.apache.org/download.cgi

###Set up your git repo

1) Fork this repository

2) Clone your repository to your local machine 

##Homework submition

When you finished required code locally, push it to your repository on github and create a pull request to this repo.

In pull request mark your mentor (@mentorName) so it can review it. 


## Hometask 1

To complite task one you need to fix DataProcessorTest unit tests. 
All methods in DataProcessor with "7" in a name should be implemented using Java 7, when other should be done with Java 8 streaming api.


## Hometask 2
The AccidentDataProcessor.processFile sequentially reads, enriches and writes accident data to a file. Serial processing takes much time. Use multithreading
to run these tasks in parallel and communicate to each other to reduce time.

**As the data size and processing complexity is low here the performance gain by multithreading might be little. To simulate complex data heavy processing
I have introduced a delay of 1 second in data reader, data enricher and data writer. The usage is via Util.sleep(). You can increase the time here or switch
it off to play around.

Below task is optional if somebody is interested to play with more.
For more complicated scenario change to use PoliceForceExternalDataService.getContactNoWithDelay() from enricher to
simulate the scenario that enrichment from 3rd party service may get stuck. As a solution this can be run in a separate
thread and the result should be returned using Future with a timeout

## Hometask 3
In this assignment you have to cover the code introduced in previous home task with tests. 
Feel free to cover both your own code or code provided to you as a part of home task 2.

### Stick to these requirements:
1. There should be **at least 3** tests implemented. The more tests you implement - the better, but please remember to avoid redundant tests. Test coverage will be taken into consideration for final projects evaluation. You can use your IDE/plugin to measure that.  
2. Make use of Hamcrest matchers in your tests
3. Use Mockito in your unit tests (or [PowerMock](https://github.com/powermock/powermock/wiki/MockitoUsage), which overcomes Mockito limitations): 
  * mock() dependecies of tested component
  * provide at least one stub with when()
  * verify() behavior at least once
4. Write at least one integration test, without mocking (hint: you may consider testing file writing functionality)
