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
import org.junit.BeforeClass;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author ahunt
 */
abstract public class OozieCoordinatorTestCase extends MiniOozieTestCase {


    private Path appPath;

    static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mmZ");

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

    List<CoordinatorAction> runDSTTest(String startDate,
                                       String stopDate,
                                       String initialDate,
                                       String coordinatorConfigPath
    ) throws IOException, OozieClientException {
        return runDSTTest(startDate, stopDate, initialDate, coordinatorConfigPath, "wf-dst-test.xml");
    }

    List<CoordinatorAction> runDSTTest(String startDate,
                                       String stopDate,
                                       String initialDate,
                                       String coordinatorConfigPath,
                                       String workflowConfigPath
    ) throws IOException, OozieClientException {
        FileSystem fs = getFileSystem();

        Reader reader = getResourceAsReader(coordinatorConfigPath);
        Writer writer = new OutputStreamWriter(fs.create(new Path(appPath, "coordinator.xml")));
        copyCharStream(reader, writer);
        writer.close();
        reader.close();

        reader = getResourceAsReader(workflowConfigPath);
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

        for (CoordinatorAction action : actions) {
            System.out.println("******* " + action.getNominalTime());
            System.out.println("******* " + action.getMissingDependencies());
        }

        return actions;
    }

    void checkDependencies(List<CoordinatorAction> actions,
                           List<Date> expectedNominalTimes,
                           List<Set<String>> expectedDependencies) {
        assertEquals(expectedNominalTimes.size(), actions.size());
        assertEquals(expectedDependencies.size(), actions.size());

        for (int i = 0; i < actions.size(); i++) {
            assertEquals(expectedNominalTimes.get(i), actions.get(i).getNominalTime());

            Set<String> dependencies = new HashSet<>();
            Collections.addAll(dependencies, actions.get(i).getMissingDependencies().split("#"));

            assertEquals("Dependencies: " + dependencies, expectedDependencies.get(i).size(), dependencies.size());
            Set<String> expected = expectedDependencies.get(i);
            for (String dependency : dependencies) {
                assertTrue("Dependency should be in set: " + dependency + " : " + actions.get(i).getNominalTime() + " : " + expected + " : " + dependencies, expected.contains(dependency));
            }
        }
    }

    private InputStream getResourceAsStream(String path) throws IOException {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        if (is == null) {
            throw new IllegalArgumentException("resource " + path + " not found");
        }
        return is;
    }

    private Reader getResourceAsReader(String path) throws IOException {
        return new InputStreamReader(getResourceAsStream(path));
    }

    private void copyCharStream(Reader reader, Writer writer) throws IOException {
        char[] buffer = new char[4096];
        int read;
        while ((read = reader.read(buffer)) > -1) {
            writer.write(buffer, 0, read);
        }
    }
}
