/*
 * Copyright (C) 2017 WSO2 Inc. (http://wso2.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.wso2.extension.siddhi.gpl.execution.nlp;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * Test case for NlpTransformProcessorTestCase.
 */
public abstract class NlpTransformProcessorTestCase {

    private static Logger logger = Logger.getLogger(NlpTransformProcessorTestCase.class);
    protected SiddhiManager siddhiManager = new SiddhiManager();

    protected long start;
    protected long end;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Thread.sleep(1000);
    }

    protected void generateEvents(SiddhiAppRuntime siddhiAppRuntime, String dataIn, List<String[]> data)
            throws Exception {
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler(dataIn);
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }

    protected void assertOutput(List<Event> outputEvents, String[] expectedMatches, int[] inStreamIndices,
                                List<String[]> data) {
        for (int i = 0; i < outputEvents.size(); i++) {
            Event event = outputEvents.get(i);
            //Compare expected output stream match and received match
            assertEquals(expectedMatches[i], event.getData(2));
            //Compare expected output stream username and received username
            assertEquals(data.get(inStreamIndices[i])[0], event.getData(0));
            //Compare expected output stream text and received text
            assertEquals(data.get(inStreamIndices[i])[1], event.getData(1));
        }
        assertNotEquals(0, outputEvents.size(), "Returns an empty event array");
    }

    protected List<Event> runQuery(String query, String queryName, String dataInStreamName, List<String[]> data)
            throws Exception {
        start = System.currentTimeMillis();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(query);
        end = System.currentTimeMillis();
        logger.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));

        final List<Event> eventList = new ArrayList<>();

        siddhiAppRuntime.addCallback(queryName, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    eventList.add(event);
                }
            }
        });
        siddhiAppRuntime.start();
        generateEvents(siddhiAppRuntime, dataInStreamName, data);

        return eventList;
    }
}
