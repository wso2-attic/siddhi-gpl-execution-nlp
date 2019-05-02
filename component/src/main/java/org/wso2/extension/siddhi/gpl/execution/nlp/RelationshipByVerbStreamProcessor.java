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
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.log4j.Logger;
import org.wso2.extension.siddhi.gpl.execution.nlp.utility.Constants;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;


/**
 * Implementation for RelationshipByVerbStreamProcessor.
 */
@Extension(
        name = "findRelationshipByVerb",
        namespace = "nlp",
        description = "This feature extracts the subject, object, and verb relationship for a given verb base form.",
        parameters = {
                @Parameter(
                        name = "verb",
                        description = "In this parameter, specify the string constant for the verb base form.",
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
                        syntax = "nlp:findRelationshipByVerb(\"say\", " +
                                "\"Information just reaching us says another Liberian With Ebola " +
                                "Arrested At Lagos Airport\")",
                        description = "This returns 4 parameters: the whole text, `Information` as the subject, " +
                                "`Liberian` as the object and \"says\" as the verb."
                )
        }
)
public class RelationshipByVerbStreamProcessor extends StreamProcessor<State> {

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
    private ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);


    private synchronized void initPipeline() {
        logger.info("Initializing Annotator pipeline ...");
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");

        pipeline = new StanfordCoreNLP(props);
        logger.info("Annotator pipeline initialized");
    }

    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
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
        attributes.add(new Attribute(Constants.SUBJECT, Attribute.Type.STRING));
        attributes.add(new Attribute(Constants.OBJECT, Attribute.Type.STRING));
        attributes.add(new Attribute(Constants.VERB, Attribute.Type.STRING));
        return null;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           State state) {
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
    public List<Attribute> getReturnAttributes() {
        return attributes;

    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
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
