/*
 * Copyright (c)  2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.wso2.extension.siddhi.gpl.execution.nlp.utility.Constants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
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
 * Implementation for RelationshipByRegexStreamProcessor.
 */
@Extension(
        name = "findRelationshipByRegex",
        namespace = "nlp",
        description = "Extract subject, object and verb from the text stream that match with the named nodes " +
                "of the Semgrex pattern.",
        parameters = {
                @Parameter(
                        name = "regex",
                        description = "User given regular expression that match the Semgrex pattern syntax.",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "text",
                        description = "A string or the stream attribute which the text stream resides.",
                        type = {DataType.STRING}
                )
        },
        examples = {
                @Example(
                        syntax = "nlp:findRelationshipByRegex" +
                                "('{}=verb >/nsubj|agent/ {}=subject >/dobj/ {}=object', " +
                                "\"gates foundation donates $50M in support of #Ebola relief\")",
                        description = "Returns 4 parameters. the whole text, subject as \"foundation\", " +
                                "object as \"$\", verb as \"donates\"."
                )
        }
)
public class RelationshipByRegexStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(RelationshipByRegexStreamProcessor.class);

    /**
     * represents {}=<word> pattern
     * used to find named nodes.
     */
    private static final String validationRegex = "(?:[{.*}]\\s*=\\s*)(\\w+)";

    private SemgrexPattern regexPattern;
    private StanfordCoreNLP pipeline;

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
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext)  {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (attributeExpressionLength < 2) {
            throw new SiddhiAppCreationException("Query expects at least two parameters. Received only " +
                    attributeExpressionLength +
                    ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
        }

        String regex;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                regex = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new SiddhiAppCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
        }

        try {
            regexPattern = SemgrexPattern.compile(regex);
        } catch (SemgrexParseException e) {
            throw new SiddhiAppCreationException("Cannot parse given regex: " + regex, e);
        }

        Set<String> namedNodeSet = new HashSet<String>();
        Pattern validationPattern = Pattern.compile(validationRegex);
        Matcher validationMatcher = validationPattern.matcher(regex);
        while (validationMatcher.find()) {
            //group 1 of the matcher gives the node name
            namedNodeSet.add(validationMatcher.group(1).trim());
        }

        if (!namedNodeSet.contains(Constants.SUBJECT)) {
            throw new SiddhiAppCreationException("Given regex " + regex +
                    " does not contain a named node as subject. " +
                    "Expect a node named as {}=subject");
        }

        if (!namedNodeSet.contains(Constants.OBJECT)) {
            throw new SiddhiAppCreationException("Given regex " + regex +
                    " does not contain a named node as object. " +
                    "Expect a node named as {}=object");
        }

        if (!namedNodeSet.contains(Constants.VERB)) {
            throw new SiddhiAppCreationException("Given regex " + regex +
                    " does not contain a named node as verb. Expect" +
                    " a node named as {}=verb");
        }

        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    inputDefinition.getAttributeList()));
        }

        initPipeline();
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);
        attributes.add(new Attribute(Constants.SUBJECT, Attribute.Type.STRING));
        attributes.add(new Attribute(Constants.OBJECT, Attribute.Type.STRING));
        attributes.add(new Attribute(Constants.VERB, Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Event received. Regex:%s Event:%s",
                            regexPattern.pattern(), streamEvent));
                }

                Annotation document = pipeline.process(attributeExpressionExecutors[1]
                        .execute(streamEvent).toString());

                SemgrexMatcher matcher;
                SemanticGraph graph;
                for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                    graph = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
                    matcher = regexPattern.matcher(graph);

                    while (matcher.find()) {
                        Object[] data = new Object[3];
                        data[0] = matcher.getNode(Constants.SUBJECT) == null ? null : matcher.getNode(Constants.SUBJECT)
                                .word();
                        data[1] = matcher.getNode(Constants.OBJECT) == null ? null : matcher.getNode(Constants.OBJECT)
                                .word();
                        data[2] = matcher.getNode(Constants.VERB) == null ? null : matcher.getNode(Constants.VERB)
                                .word();
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
