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


/**
 * Implementation for RelationshipByVerbStreamProcessor.
 */
@Extension(
        name = "findRelationshipByVerb",
        namespace = "nlp",
        description = "Extract subject, object, verb relationship for a given verb base form.",
        parameters = {
                @Parameter(
                        name = "verb",
                        description = "User given string constant for the verb base form.",
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
                        syntax = "nlp:findRelationshipByVerb(\"say\", " +
                                "\"Information just reaching us says another Liberian With Ebola " +
                                "Arrested At Lagos Airport\")",
                        description = "Returns 4 parameters. the whole text, subject as " +
                                "Information, object as Liberian, verb as \"says\"."
                )
        }
)
public class RelationshipByVerbStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(RelationshipByVerbStreamProcessor.class);

    /**
     * Used to find subject, object and verb, where subject is optional.
     */
    private static final String verbOptSub = "{lemma:%s}=verb ?>/nsubj|agent|xsubj/ {}=subject " +
            ">/dobj|iobj|nsubjpass/ {}=object";
    /**
     * Used to find subject, object and verb, where object is optional.
     */
    private static final String verbOptObj = "{lemma:%s}=verb >/nsubj|agent|xsubj/ {}=subject " +
            "?>/dobj|iobj|nsubjpass/ {}=object";

    private SemgrexPattern verbOptSubPattern;
    private SemgrexPattern verbOptObjPattern;
    private String verb;
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
                    ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        String verb;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                verb = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new SiddhiAppCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        try {
            verbOptSubPattern = SemgrexPattern.compile(String.format(verbOptSub, verb));
            verbOptObjPattern = SemgrexPattern.compile(String.format(verbOptObj, verb));
        } catch (SemgrexParseException e) {
            throw new SiddhiAppCreationException("First parameter is not a verb. Found " + verb +
                    "\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. verb: %s Stream Parameters: %s", verb,
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
                    logger.debug(String.format("Event received. Verb:%s Event:%s", verb, streamEvent));
                }

                Annotation document = pipeline.process(attributeExpressionExecutors[1].execute(streamEvent).toString());

                Set<Event> eventSet = new HashSet<Event>();

                for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                    findMatchingEvents(sentence, verbOptSubPattern, eventSet);
                    findMatchingEvents(sentence, verbOptObjPattern, eventSet);
                }

                for (Event event : eventSet) {
                    Object[] data = new Object[3];
                    data[0] = event.subject;
                    data[1] = event.object;
                    data[2] = event.verb;
                    StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    complexEventPopulater.populateComplexEvent(newStreamEvent, data);
                    streamEventChunk.insertBeforeCurrent(newStreamEvent);
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

    private static final class Event {
        String subject;
        String object;
        String verb;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Event event = (Event) o;

            if (object != null ? !object.equals(event.object) : event.object != null) {
                return false;
            }

            if (subject != null ? !subject.equals(event.subject) : event.subject != null) {
                return false;
            }

            return verb.equals(event.verb);
        }

        @Override
        public int hashCode() {
            int result = subject != null ? subject.hashCode() : 0;
            result = 31 * result + (object != null ? object.hashCode() : 0);
            result = 31 * result + (verb != null ? verb.hashCode() : 0);
            return result;
        }
    }
    
    private void findMatchingEvents(CoreMap sentence, SemgrexPattern pattern, Set<Event> eventSet) {
        SemanticGraph graph = sentence.get(
                SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
        SemgrexMatcher matcher = pattern.matcher(graph);

        while (matcher.find()) {
            Event event = new Event();
            event.verb = matcher.getNode(Constants.VERB) == null ? null : matcher.getNode(Constants.VERB).word();
            event.subject = matcher.getNode(Constants.SUBJECT) == null ? null :
                                                            matcher.getNode(Constants.SUBJECT).word();
            event.object = matcher.getNode(Constants.OBJECT) == null ? null : matcher.getNode(Constants.OBJECT).word();

            eventSet.add(event);
        }
    }
}
