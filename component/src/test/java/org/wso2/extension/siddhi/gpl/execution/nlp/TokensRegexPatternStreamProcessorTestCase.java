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

import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

/**
 * Test case for TokensRegexPatternStreamProcessor.
 */
public class TokensRegexPatternStreamProcessorTestCase extends NlpTransformProcessorTestCase {
    static List<String[]> data = new ArrayList<>();
    private static Logger logger = Logger.getLogger(TokensRegexPatternStreamProcessorTestCase.class);
    private static String defineStream = "define stream TokenRegexPatternIn(regex string, text string);";

    @BeforeClass
    public static void loadData() throws Exception {

        data.add(new String[]{"Professeur Jamelski",
                "Bill Gates donates $31million to fight Ebola http://t.co/Lw8iJUKlmw http://t.co/wWVGNAvlkC"});
        data.add(new String[]{"going info",
                "Trail Blazers owner Paul Allen donates $9million to fight Ebola"});
        data.add(new String[]{"WinBuzzer",
                "Microsoft co-founder Paul Allen donates $9million to help fight Ebola in Africa"});
        data.add(new String[]{"Lillie Lynch",
                "Canada to donate $2.5M for protective equipment http://t.co/uvRcHSYY0e"});
        data.add(new String[]{"Sark Crushing On Me",
                "Bill Gates donate $50 million to fight ebola in west africa http://t.co/9nd3viiZbe"});
    }

    @Test
    public void testFindTokensRegexPatternMatch() throws Exception {
        //expecting matches
        String[] expectedMatches = {"Bill Gates donates $31million",
                "Paul Allen donates $9million",
                "Microsoft co-founder Paul Allen donates $9million",
                "Canada to donate $2.5",
                "Bill Gates donate $50 million"};
        //expecting group_1
        String[] expectedGroup1 = {"Bill Gates", "Paul Allen", "Microsoft", "Canada", "Bill Gates"};
        //expecting group_2
        String[] expectedGroup2 = {"$31million", "$9million", "$9million", "$2.5", "$50 million"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0, 1, 2, 3, 4};

        List<Event> outputEvents = testFindTokensRegexPatternMatch(
                "([ner:/PERSON|ORGANIZATION|LOCATION/]+) (?:[]* [lemma:donate]) ([ner:MONEY]+)");

        for (int i = 0; i < outputEvents.size(); i++) {
            Event event = outputEvents.get(i);
            //Compare expected subject and received subject
            assertEquals(expectedMatches[i], event.getData(2));
            //Compare expected object and received object
            assertEquals(expectedGroup1[i], event.getData(3));
            //Compare expected verb and received verb
            assertEquals(expectedGroup2[i], event.getData(4));
            //Compare expected output stream username and received username
            assertEquals(data.get(matchedInStreamIndices[i])[0], event.getData(0));
            //Compare expected output stream text and received text
            assertEquals(data.get(matchedInStreamIndices[i])[1], event.getData(1));
        }
        assertNotEquals(0, outputEvents.size(), "Returns an empty event array");

    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.createSiddhiAppRuntime(defineStream + "from TokenRegexPatternIn#nlp:findTokensRegexPattern" +
                "        ( text) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testQueryCreationExceptionRegexCannotParse() {
        logger.info("Test: QueryCreationException at Regex parsing");
        siddhiManager.createSiddhiAppRuntime(defineStream + "from TokenRegexPatternIn#nlp:findTokensRegexPattern" +
                "        ( '/{tag:NP.*}/',text) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n");
    }

    private List<Event> testFindTokensRegexPatternMatch(String regex) throws Exception {
        logger.info(String.format("Test: Regex = %s", regex));
        String query = "@info(name = 'query1') from TokenRegexPatternIn#nlp:findTokensRegexPattern" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n";
        start = System.currentTimeMillis();
        return runQuery(defineStream + String.format(query, regex), "query1", "TokenRegexPatternIn", data);
    }
}
