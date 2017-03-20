package com.riskiq.oozie;

import com.google.common.collect.ImmutableList;
import org.apache.oozie.client.CoordinatorAction;

import java.util.*;

/**
 * @author ahunt
 */
public class HourlyToDailyTests extends OozieCoordinatorTestCase {

    private String coordConfig = "coord-utc-hourly_in-daily_out-test.xml";
    private String initialTime = "2014-06-05T00:00-0800";

    public void testPDTtoPST() throws Exception {
        List<CoordinatorAction> actions = runDSTTest(
                "2015-03-06T17:00-0800",
                "2015-03-11T17:00-0800",
                initialTime,
                coordConfig);

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-03-06T17:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-07T17:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-08T17:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-09T17:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-10T17:00-0800"));

        List<String> days = ImmutableList.of("20150306", "20150307", "20150308", "20150309", "20150310");
        List<Set<String>> expectedDependencies = generatedHourlyDependencies(days);

        checkDependencies(actions,
                expectedNominalTimes,
                expectedDependencies);
    }

    public void testPSTtoPDT() throws Exception {
        List<CoordinatorAction> actions = runDSTTest(
                "2015-10-29T18:00-0800",
                "2015-11-03T17:00-0800",
                initialTime,
                coordConfig);

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-10-29T18:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-10-30T18:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-10-31T18:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-01T18:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-02T18:00-0800"));

        List<String> days = ImmutableList.of("20151029", "20151030", "20151031", "20151101", "20151102");
        List<Set<String>> expectedDependencies = generatedHourlyDependencies(days);

        checkDependencies(actions, expectedNominalTimes, expectedDependencies);
    }

    private List<Set<String>> generatedHourlyDependencies(List<String> days) {
        List<Set<String>> expectedDependencies = new ArrayList<>();
        Set<String> dependencies;
        for (String day: days) {
            dependencies = new HashSet<>();
            dependencies.add(String.format("hdfs:///user/test/testDaily/day=%s/_SUCCESS", day));
            for (int i = 0; i < 24; i++) {
                dependencies.add(String.format("hdfs:///user/test/testHourly/hour=%s%02d/_SUCCESS", day, i));
            }
            expectedDependencies.add(dependencies);
        }

        return expectedDependencies;
    }
}
