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
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Implementation for TokensRegexPatternStreamProcessor.
 */
@Extension(
        name = "findTokensRegexPattern",
        namespace = "nlp",
        description = "This feature extracts groups (defined in the Semgrex pattern) from the text stream.",
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
                        name = "groupNum1",
                        description = "First group match of the regex. Group numbers vary dynamically with the " +
                                "number of capturing groups in the regex pattern.",
                        type = {DataType.STRING}
                )
        },
        examples = {
                @Example(
                        syntax = "nlp:findTokensRegexPattern" +
                                "('([ner:/PERSON|ORGANIZATION|LOCATION/]+) (?:[]* [lemma:donate]) ([ner:MONEY]+)'" +
                                ", text) ",
                        description = "This returns 3 parameters: the whole text, \"Paul Allen donates " +
                                "$ 9million\" as the match, \"Paul Allen\" as the group number, and \"$ 9million\" " +
                                "as group number 2. It defines three groups, and the middle group is defined as a " +
                                "non-capturing group. The first group looks for words that are entities of either" +
                                "`PERSON`, `ORGANIZATON` or `LOCATION` with one or more successive words matching the" +
                                " same. The second group represents any number of words that are followed by a word" +
                                " with a lemmatization for donate such as `donates`, `donated`, `donating` etc. The " +
                                "third looks for one or more successive entities of the `MONEY` type."
                )
        }
)
public class TokensRegexPatternStreamProcessor extends StreamProcessor<State> {

    private static Logger logger = Logger.getLogger(TokensRegexPatternStreamProcessor.class);

    private static final String groupPrefix = "groupNum";
    private int attributeCount;
    private TokenSequencePattern regexPattern;
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
                    ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
        }

        String regex;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                regex = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new SiddhiAppCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            throw new SiddhiAppCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
        }

        try {
            regexPattern = TokenSequencePattern.compile(regex);
        } catch (Exception e) {
            throw new SiddhiAppCreationException("Cannot parse given regex " + regex, e);
        }


        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    inputDefinition.getAttributeList()));
        }

        initPipeline();

        attributes.add(new Attribute("match", Attribute.Type.STRING));
        attributeCount = regexPattern.getTotalGroups();
        for (int i = 1; i < attributeCount; i++) {
            attributes.add(new Attribute(groupPrefix + i, Attribute.Type.STRING));
        }
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
                    logger.debug(String.format("Event received. Regex:%s Event:%s",
                            regexPattern.pattern(), streamEvent));
                }

                Annotation document = pipeline.process(attributeExpressionExecutors[1].execute(streamEvent).toString());

                for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                    TokenSequenceMatcher matcher = regexPattern.getMatcher(
                            sentence.get(CoreAnnotations.TokensAnnotation.class));
                    while (matcher.find()) {
                        Object[] data = new Object[attributeCount];
                        data[0] = matcher.group();
                        for (int i = 1; i < attributeCount; i++) {
                            data[i] = matcher.group(i);
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
    public List<Attribute> getReturnAttributes() {
        return attributes;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }
}
