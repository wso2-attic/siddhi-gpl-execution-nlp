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
import org.wso2.extension.siddhi.gpl.execution.nlp.utility.Constants;
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
 * Implementation for RelationshipByRegexStreamProcessor.
 */
@Extension(
        name = "findRelationshipByRegex",
        namespace = "nlp",
        description = "This extension extracts subject, object and verb from the text stream that matches the named " +
                "nodes of the Semgrex pattern.",
        parameters = {
                @Parameter(
                        name = "regex",
                        description = "In this parameter, specify the regular expression that matches the Semgrex " +
                                "pattern syntax.",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "text",
                        description = "The string or the stream attribute in which the text stream resides.",
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
                        name = "subject",
                        description = "The matched subject in the text.",
                        type = {DataType.STRING}
                ),
                @ReturnAttribute(
                        name = "object",
                        description = "The matched object in the text.",
                        type = {DataType.STRING}
                ),
                @ReturnAttribute(
                        name = "verb",
                        description = "The matched verb in the text.",
                        type = {DataType.STRING}
                )
        },
        examples = {
                @Example(
                        syntax = "nlp:findRelationshipByRegex" +
                                "('{}=verb >/nsubj|agent/ {}=subject >/dobj/ {}=object', " +
                                "\"gates foundation donates $50M in support of #Ebola relief\")",
                        description = "This returns 4 parameters: the whole text, \"foundation\" as the subject, " +
                                "\"$\" as the object, and \"donates\" as the verb."
                )
        }
)
public class RelationshipByRegexStreamProcessor extends StreamProcessor {

    /**
     * represents {}=<word> pattern
     * used to find named nodes.
     */
    private static final String validationRegex = "(?:[{.*}]\\s*=\\s*)(\\w+)";
    private static Logger logger = Logger.getLogger(RelationshipByRegexStreamProcessor.class);
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
                                   ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
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
