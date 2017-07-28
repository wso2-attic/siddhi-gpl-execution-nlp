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

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.semgrex.SemgrexMatcher;
import edu.stanford.nlp.semgraph.semgrex.SemgrexParseException;
import edu.stanford.nlp.semgraph.semgrex.SemgrexPattern;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of SemgrexPatternStreamProcessor.
 */
@Extension(
        name = "findSemgrexPattern",
        namespace = "nlp",
        description = "This extension extracts named nodes (through the Semgrex pattern) from the text stream.",
        parameters = {
                @Parameter(
                        name = "regex",
                        description = "In this parameter, specify the regular expression that matches the Semgrex " +
                                "pattern syntax.",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "text",
                        description = "A string or the stream attribute in which the text stream resides.",
                        type = {DataType.STRING}
                )
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "match",
                        description = "The entire matched text.",
                        type = {DataType.STRING}
                ),
                @ReturnAttribute(
                        name = "dynamicMatchName1",
                        description = "The matched groups in the regex. The name of the return attribute varies " +
                                "based on the group names in the given regex.",
                        type = {DataType.STRING}
                )
        },
        examples = {
                @Example(
                        syntax = "nlp:findSemgrexPattern" +
                                "('{lemma:die} >/.*subj|num.*/=reln {}=diedsubject', " +
                                "\"Sierra Leone doctor dies of Ebola after failed evacuation.\") ",
                        description = "This returns 3 parameters:  \"Sierra Leone doctor dies of Ebola after failed" +
                                " evacuation.\" as the `whole text match`, \"nsubj\" as `reln`,  and \"doctor\" as " +
                                "`diedsubject`. This looks for words with lemmetizations for `die` that are " +
                                "governors on any subject or numeric relation. The dependent is marked as the " +
                                "`diedsubject` and the relationship is marked as `reln`. Thus, the query returns an " +
                                "output stream that outputs the full match of this expression, i.e the governing word" +
                                " with the lemmatization for `die`. In addition, it outputs the `diedsubject` named" +
                                " node and the `reln` named relation for each match it finds."
                )
        }
)
public class SemgrexPatternStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(SemgrexPatternStreamProcessor.class);
    /**
     * represents =<word> pattern
     * used to find named nodes and named relations.
     */
    private static final String validationRegex = "(?:\\s*=\\s*)(\\w+)";

    private int attributeCount;
    private int inStreamParamPosition;
    private SemgrexPattern regexPattern;
    private StanfordCoreNLP pipeline;
    private Map<String, Integer> namedElementParamPositions = new HashMap<String, Integer>();

    private synchronized void initPipeline() {
        logger.info("Initializing Annotator pipeline ...");
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");

        pipeline = new StanfordCoreNLP(props);
        logger.info("Annotator pipeline initialized");
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (attributeExpressionLength < 2) {
            throw new SiddhiAppCreationException("Query expects at least two parameters. Received only " +
                    attributeExpressionLength +
                    ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
        }

        String regex;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                regex = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new SiddhiAppCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
        }

        try {
            regexPattern = SemgrexPattern.compile(regex);
        } catch (SemgrexParseException e) {
            throw new SiddhiAppCreationException("Cannot parse given regex: " + regex, e);
        }


        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    inputDefinition.getAttributeList()));
        }

        initPipeline();

        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);
        attributes.add(new Attribute("match", Attribute.Type.STRING));


        // Find all named elements in the regular expression and add them to the output stream definition attributes
        Set<String> namedElementSet = new HashSet<String>();
        Pattern validationPattern = Pattern.compile(validationRegex);
        Matcher validationMatcher = validationPattern.matcher(regex);
        while (validationMatcher.find()) {
            //group 1 of the matcher gives the node name or the relation name
            namedElementSet.add(validationMatcher.group(1).trim());
        }

        attributeCount = 1;
        for (String namedElement : namedElementSet) {
            attributes.add(new Attribute(namedElement, Attribute.Type.STRING));
            namedElementParamPositions.put(namedElement, attributeCount++);
        }
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                Annotation document = pipeline.process(attributeExpressionExecutors[1].execute(streamEvent).toString());

                for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                    SemanticGraph graph = sentence.get(
                            SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
                    SemgrexMatcher matcher = regexPattern.matcher(graph);

                    while (matcher.find()) {
                        Object[] data = new Object[attributeCount];
                        data[0] = matcher.getMatch().value();

                        for (String nodeName : matcher.getNodeNames()) {
                            if (namedElementParamPositions.containsKey(nodeName)) {
                                data[namedElementParamPositions.get(nodeName)] = matcher.getNode(nodeName) == null
                                        ? null : matcher.getNode(nodeName).word();
                            }
                        }

                        for (String relationName : matcher.getRelationNames()) {
                            if (namedElementParamPositions.containsKey(relationName)) {
                                data[namedElementParamPositions.get(relationName)] =
                                        matcher.getRelnString(relationName);
                            }
                        }
                        StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        complexEventPopulater.populateComplexEvent(newStreamEvent, data);
                        streamEventChunk.insertBeforeCurrent(newStreamEvent);
                    }
                }
                streamEventChunk.remove();
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, Object> currentState() {
        return new HashMap<>();
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }
}
