package com.riskiq.oozie;

import com.google.common.collect.ImmutableSet;
import org.apache.oozie.client.CoordinatorAction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Tests daily job that processes previous days data where datasets use LA time zone.
 * @author ahunt
 */
public class DSTTests extends OozieCoordinatorTestCase {

    private String coordConfig = "coord-dst-test.xml";
    private String initialTime = "2014-06-05T00:00-0800";

    public void testPDTtoPST() throws Exception {
        List<CoordinatorAction> actions = runDSTTest(
                "2015-03-07T01:00-0800",
                "2015-03-11T05:45-0800",
                initialTime,
                coordConfig);

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-03-07T01:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-08T01:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-03-09T01:00-0700"));
        expectedNominalTimes.add(sdf.parse("2015-03-10T01:00-0700"));
        expectedNominalTimes.add(sdf.parse("2015-03-11T01:00-0700"));

        List<Set<String>> expectedDependencies = new ArrayList<>();
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20150306/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20150307/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20150308/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20150309/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20150310/_SUCCESS"));

        checkDependencies(actions, expectedNominalTimes, expectedDependencies);
    }

    public void testPSTtoPDT() throws Exception {
        List<CoordinatorAction> actions = runDSTTest(
                "2015-10-30T00:00-0800",
                "2015-11-03T20:00-0800",
                initialTime, coordConfig);

        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-10-30T00:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-10-31T00:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-01T00:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-02T01:00-0800"));
        expectedNominalTimes.add(sdf.parse("2015-11-03T01:00-0800"));

        List<Set<String>> expectedDependencies = new ArrayList<>();
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20151029/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20151030/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20151031/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20151101/_SUCCESS"));
        expectedDependencies.add(ImmutableSet.of("hdfs:///user/test/testDaily/day=20151102/_SUCCESS"));

        checkDependencies(actions,
                expectedNominalTimes,
                expectedDependencies);
    }
}
