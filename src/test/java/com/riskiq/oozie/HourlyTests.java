package com.riskiq.oozie;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.oozie.client.CoordinatorAction;

import java.util.*;

/**
 * @author ahunt
 */
public class HourlyTests extends OozieCoordinatorTestCase {

    private String coordConfig = "coord-hourly-utc-example.xml";
    private String initialTime = "2014-06-05T00:00-0800";

    public void testOneHourPST() throws Exception {
        List<CoordinatorAction> actions = runDSTTest(
                "2015-03-07T00:10-0800",
                "2015-03-07T01:00-0800",
                initialTime,
                coordConfig);

        assertEquals(1, actions.size());

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-03-07T00:10-0800"));

        List<Set<String>> expectedDependencies = new ArrayList<>();
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testHourly/hour=2015030623/_SUCCESS"));

        checkDependencies(actions,
                expectedNominalTimes,
                expectedDependencies);
    }

    public void testOneHourPDT() throws Exception {
        List<CoordinatorAction> actions = runDSTTest(
                "2015-03-11T00:10-0800",
                "2015-03-11T01:10-0800",
                initialTime,
                coordConfig);

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-03-11T00:10-0800"));

        List<Set<String>> expectedDependencies = new ArrayList<>();
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testHourly/hour=2015031023/_SUCCESS"));

        checkDependencies(actions,
                expectedNominalTimes,
                expectedDependencies);
    }


    public void testPDTtoPST() throws Exception {
        List<CoordinatorAction> actions = runDSTTest("2015-03-07T02:45-0800",
                "2015-03-11T02:45-0800",
                initialTime,
                coordConfig);

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-03-07T02:45-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-08T02:45-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-09T02:45-0700"));
        expectedNominalTimes.add(sdf.parse("2015-03-10T02:45-0700"));
        expectedNominalTimes.add(sdf.parse("2015-03-11T02:45-0700"));

        List<String> days = ImmutableList.of("20150306", "20150307", "20150308", "20150309", "20150310");
        List<Set<String>> expectedDependencies = generatedHourlyDependencies(days);

        checkDependencies(actions,
                expectedNominalTimes,
                expectedDependencies);
    }

    public void testPSTtoPDT() throws Exception {
        List<CoordinatorAction> actions = runDSTTest("2015-10-30T00:00-0800",
                "2015-11-03T20:00-0800",
                initialTime,
                coordConfig);

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-10-30T00:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-10-31T00:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-01T00:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-02T00:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-03T00:00-0800"));

        List<String> days = ImmutableList.of("20151029", "20151030", "20151031", "20151101", "20151102");
        List<Set<String>> expectedDependencies = generatedHourlyDependencies(days);

        checkDependencies(actions, expectedNominalTimes, expectedDependencies);
    }

    private List<Set<String>> generatedHourlyDependencies(List<String> days) {
        List<Set<String>> expectedDependencies = new ArrayList<>();
        Set<String> dependencies;
        for (String day: days) {
            dependencies = new HashSet<>();
            for (int i = 0; i < 24; i++) {
                dependencies.add(String.format("hdfs:///user/test/testHourly/hour=%s%02d/_SUCCESS", day, i));
            }
            expectedDependencies.add(dependencies);
        }

        return expectedDependencies;
    }
}
