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
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
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
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Stream processor implementation for name entity.
 */
@Extension(
        name = "findNameEntityType",
        namespace = "nlp",
        description = "Find the entities in the text by the given type.",
        parameters = {
                @Parameter(
                        name = "entity.type",
                        description = "User given string constant as entity Type. Possible Values : PERSON, " +
                                "LOCATION, ORGANIZATION, MONEY, PERCENT, DATE or TIME",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "group.successive.match",
                        description = "User given boolean constant in order to group successive matches of the " +
                                "given entity type and a text stream.",
                        type = {DataType.BOOL}
                ),
                @Parameter(
                        name = "text",
                        description = "A string or the stream attribute which the text stream resides.",
                        type = {DataType.STRING}
                )
        },
        returnAttributes = {
          @ReturnAttribute(
                  name = "match",
                  description = "Event returns a single match. If multiple matches are found multiple events are " +
                          "returned each containing a single match.",
                  type = {DataType.STRING}
          )
        },
        examples = {
                @Example(
                        syntax = "nlp:findNameEntityType(\"PERSON\",true,text)",
                        description = "If text attribute contains \"Bill Gates donates £31million to fight Ebola.\" " +
                                "result will be \"Bill Gates\". If groupSuccessiveMatch is \"false\" two events will " +
                                "be generated as \"Bill\" and \"Gates\"."
                )
        }
)
public class NameEntityTypeStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(NameEntityTypeStreamProcessor.class);

    private Constants.EntityType entityType;
    private boolean groupSuccessiveEntities;
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

        if (attributeExpressionLength < 3) {
            throw new SiddhiAppCreationException("Query expects at least three parameters. Received only " +
                    attributeExpressionLength +
                    ".\nUsage: #nlp:findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }

        String entityTypeParam;
        try {
            entityTypeParam = (attributeExpressionExecutors[0]).execute(null).toString();
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }

        try {
            this.entityType = Constants.EntityType.valueOf(entityTypeParam.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new SiddhiAppCreationException("First parameter should be one of " + Arrays.deepToString(Constants
                    .EntityType.values()) + ". Found " + entityTypeParam);
        }

        try {
            groupSuccessiveEntities = (Boolean) (attributeExpressionExecutors[1]).execute(null);
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("Second parameter should be of type boolean. Found " +
                    attributeExpressionExecutors[1].getReturnType() +
                    ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. EntityType: %s GroupSuccessiveEntities %s " +
                            "Stream Parameters: %s", entityTypeParam, groupSuccessiveEntities,
                    inputDefinition.getAttributeList()));
        }

        if (!(attributeExpressionExecutors[2] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("Third parameter should be a variable. Found " +
                    attributeExpressionExecutors[2].getReturnType() +
                    ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }

        initPipeline();
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);
        attributes.add(new Attribute("match", Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();

                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Event received. Entity Type:%s GroupSuccessiveEntities:%s " +
                            "Event:%s", entityType.name(), groupSuccessiveEntities, event));
                }

                Annotation document = new Annotation(attributeExpressionExecutors[2].execute(event).toString());
                pipeline.annotate(document);

                StreamEvent newEvent = null;
                List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

                if (groupSuccessiveEntities) {
                    String word;
                    String matchedWord = null;
                    boolean isAdded = false;

                    for (CoreMap sentence : sentences) {
                        for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                            if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                                word = token.get(CoreAnnotations.TextAnnotation.class);
                                if (isAdded) {
                                    word = matchedWord + " " + word;
                                    complexEventPopulater.populateComplexEvent(newEvent, new Object[]{word});
                                } else {
                                    newEvent = streamEventCloner.copyStreamEvent(event);
                                    complexEventPopulater.populateComplexEvent(newEvent, new Object[]{word});
                                    streamEventChunk.insertBeforeCurrent(newEvent);
                                    isAdded = true;
                                    matchedWord = word;
                                }
                            } else {
                                isAdded = false;
                                matchedWord = null;
                            }
                        }
                    }
                } else {
                    for (CoreMap sentence : sentences) {
                        for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                            if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                                String word = token.get(CoreAnnotations.TextAnnotation.class);
                                newEvent = streamEventCloner.copyStreamEvent(event);
                                complexEventPopulater.populateComplexEvent(newEvent, new Object[]{word});
                                streamEventChunk.insertBeforeCurrent(newEvent);
                            }
                        }
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
