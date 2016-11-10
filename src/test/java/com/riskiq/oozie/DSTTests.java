package com.riskiq.oozie;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.test.MiniOozieTestCase;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @author ahunt
 */
public class DSTTests extends MiniOozieTestCase {

    private Path appPath;


    @Override
    protected void setUp() throws Exception {
        System.setProperty("oozie.test.metastore.server", "false");
        System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");

        super.setUp();

        FileSystem fs = getFileSystem();
        appPath = new Path(getFsTestCaseDir(), "app");
        fs.mkdirs(appPath);
        fs.mkdirs(new Path(appPath, "lib"));
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }


    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm'Z'");

    public void testPSTtoPDT() throws Exception {
        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-03-07T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-03-08T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-03-09T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-03-10T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-03-11T00:00Z"));

        List<String> expectedDependencies = new ArrayList<>();
        expectedDependencies.add("hdfs:///user/test/testDST/day=20150306/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20150307/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20150308/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20150309/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20150310/_SUCCESS");

        runDSTTest("2015-03-07T08:00Z",
                "2015-03-11T08:00Z",
                "2015-03-06T08:00Z",
                expectedNominalTimes,
                expectedDependencies);
    }

    public void testPDTtoPST() throws Exception {
        List<Date> expectedNominalTimes = new ArrayList<>();
        expectedNominalTimes.add(sdf.parse("2015-10-30T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-10-31T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-11-01T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-11-02T00:00Z"));
        expectedNominalTimes.add(sdf.parse("2015-11-03T00:00Z"));

        List<String> expectedDependencies = new ArrayList<>();
        expectedDependencies.add("hdfs:///user/test/testDST/day=20151029/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20151030/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20151031/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20151101/_SUCCESS");
        expectedDependencies.add("hdfs:///user/test/testDST/day=20151102/_SUCCESS");

        runDSTTest("2015-10-30T07:00Z", "2015-11-04T08:00Z", "2015-03-06T08:00Z", expectedNominalTimes, expectedDependencies);
    }

    private void runDSTTest(String startDate,
                            String stopDate,
                            String initialDate,
                            List<Date> expectedNominalTimes,
                            List<String> expectedDependencies) throws IOException, OozieClientException {
        FileSystem fs = getFileSystem();

        Reader reader = getResourceAsReader("coord-dst-test.xml", -1);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "coordinator.xml")));
        copyCharStream(reader, writer);
        writer.close();
        reader.close();

        reader = getResourceAsReader("wf-dst-test.xml", -1);
        writer = new OutputStreamWriter(fs.create(new Path(appPath, "workflow.xml")));
        copyCharStream(reader, writer);
        writer.close();
        reader.close();

        final OozieClient client = LocalOozie.getClientCoord("test");

        Properties coordProps = client.createConfiguration();
        coordProps.setProperty(OozieClient.COORDINATOR_APP_PATH, new Path(appPath, "coordinator.xml").toString());
        coordProps.setProperty("start", startDate);
        coordProps.setProperty("end", stopDate);
        coordProps.setProperty("initialTime", initialDate);
        coordProps.setProperty("workflowPath", appPath.toString());

        final String coordID = client.run(coordProps);
        assertNotNull(coordID);
        CoordinatorJob job = client.getCoordJobInfo(coordID);
        assertNotNull(job);
        assertEquals(job.getStatus(), CoordinatorJob.Status.PREP);

        waitFor(60 * 1000, new Predicate() {
            public boolean evaluate() throws Exception {
                CoordinatorJob job = client.getCoordJobInfo(coordID);
                return job.getStatus() == CoordinatorJob.Status.RUNNING;
            }
        });

        job = client.getCoordJobInfo(coordID);
        List<CoordinatorAction> actions = job.getActions();

        assertEquals(expectedNominalTimes.size(), actions.size());
        assertEquals(expectedDependencies.size(), actions.size());

        for (int i = 0; i < actions.size(); i++) {
            assertEquals(expectedNominalTimes.get(i), actions.get(i).getNominalTime());
            assertEquals(expectedDependencies.get(i), actions.get(i).getMissingDependencies());
        }

        for (CoordinatorAction action : actions) {
            System.out.println("******* " + action.getNominalTime());
            System.out.println("******* " + action.getMissingDependencies());
        }
    }


    public InputStream getResourceAsStream(String path, int maxLen) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) {
            throw new IllegalArgumentException("resource " + path + " not found");
        }
        return is;
    }

    public Reader getResourceAsReader(String path, int maxLen) throws IOException {
        return new InputStreamReader(getResourceAsStream(path, maxLen));
    }

    public void copyCharStream(Reader reader, Writer writer) throws IOException {
        char[] buffer = new char[4096];
        int read;
        while ((read = reader.read(buffer)) > -1) {
            writer.write(buffer, 0, read);
        }
    }
}
