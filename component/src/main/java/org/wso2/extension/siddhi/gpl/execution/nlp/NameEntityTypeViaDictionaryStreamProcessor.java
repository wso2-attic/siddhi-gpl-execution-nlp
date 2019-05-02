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
import org.wso2.extension.siddhi.gpl.execution.nlp.dictionary.Dictionary;
import org.wso2.extension.siddhi.gpl.execution.nlp.utility.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Implementation for NameEntityTypeViaDictionaryStreamProcessor.
 */
@Extension(
        name = "findNameEntityTypeViaDictionary",
        namespace = "nlp",
        description = "This feature finds the entities in the text that have been defined in the dictionary.",
        parameters = {
                @Parameter(
                        name = "entity.type",
                        description = "The string constant given as the entity type. Possible Values : `PERSON`, " +
                                "`LOCATION`, `ORGANIZATION`, `MONEY`, `PERCENT`, `DATE` or `TIME`",
                        type = {DataType.STRING}
                ),
                @Parameter(
                        name = "dictionary.file.path",
                        description = "The path to the dictionary where the expected entities for the entity types " +
                                "and the dictionary should exist in the following form,\n" +
                                "<dictionary>" +
                                "<entity id=\"PERSON\">" +
                                "<entry>Bill</entry>" +
                                "<entry>Addison</entry>" +
                                "</entity>" +
                                "</dictionary>",
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
                        description = " Each event returns a single match. If multiple matches are found, multiple" +
                                " events are returned, each containing a single match.",
                        type = {DataType.STRING}
                )
        },
        examples = {
                @Example(
                        syntax = "nlp:findNameEntityTypeViaDictionary(\"PERSON\",\"dictionary.xml\",text)",
                        description = "If the text attribute contains \"Bill Gates donates £31million to fight " +
                                "Ebola\", and the dictionary consists of the above entries, the result is \"Bill\"."
                )
        }
)
public class NameEntityTypeViaDictionaryStreamProcessor extends StreamProcessor<State> {

    private static Logger logger = Logger.getLogger(NameEntityTypeViaDictionaryStreamProcessor.class);

    private Constants.EntityType entityType;
    private Dictionary dictionary;
    private ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);


    @Override
    protected StateFactory<State> init(MetaStreamEvent metaStreamEvent, AbstractDefinition inputDefinition,
                                       ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                       StreamEventClonerHolder streamEventClonerHolder,
                                       boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                       SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionLength < 3) {
            throw new SiddhiAppCreationException("Query expects at least three parameters. Received only " +
                    attributeExpressionLength + ".\nUsage: #nlp:findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable)");
        }

        String entityTypeParam;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                entityTypeParam = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new SiddhiAppCreationException("First parameter should be a constant.");
            }
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable");
        }

        try {
            this.entityType = Constants.EntityType.valueOf(entityTypeParam.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new SiddhiAppCreationException("First parameter should be one of " + Arrays.deepToString(Constants
                    .EntityType.values()) + ". Found " + entityTypeParam);
        }

        String dictionaryFilePath;
        try {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                dictionaryFilePath = (String) attributeExpressionExecutors[1].execute(null);
            } else {
                throw new SiddhiAppCreationException("Second parameter should be a constant.");
            }
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("Second parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable");
        }

        try {
            dictionary = new Dictionary(entityType, dictionaryFilePath);
        } catch (Exception e) {
            throw new SiddhiAppCreationException("Failed to initialize dictionary.", e);
        }

        if (!(attributeExpressionExecutors[2] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("Third parameter should be a variable. Found " +
                    attributeExpressionExecutors[2].getReturnType() +
                    ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. EntityType: %s DictionaryFilePath: %s " +
                            "Stream Parameters: %s", entityTypeParam, dictionaryFilePath,
                    inputDefinition.getAttributeList()));
        }

        attributes.add(new Attribute("match", Attribute.Type.STRING));
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
                    logger.debug(String.format("Event received. Entity Type:%s DictionaryFilePath:%s Event:%s",
                            entityType.name(), dictionary.getXmlFilePath(), streamEvent));
                }

                String text = attributeExpressionExecutors[2].execute(streamEvent).toString();
                ArrayList<String> dictionaryEntries = dictionary.getEntries(entityType);

                if (dictionaryEntries.size() == 0) {
                    streamEventChunk.remove();
                } else if (dictionaryEntries.size() == 1) {
                    complexEventPopulater.populateComplexEvent(streamEvent, new Object[]{dictionaryEntries.get(0)});
                } else {
                    for (String entry : dictionaryEntries) {
                        if (text.contains(entry)) {
                            StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                            complexEventPopulater.populateComplexEvent(newStreamEvent, new Object[]{entry});
                            streamEventChunk.insertBeforeCurrent(newStreamEvent);
                        }
                    }
                    streamEventChunk.remove();
                }
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
}
