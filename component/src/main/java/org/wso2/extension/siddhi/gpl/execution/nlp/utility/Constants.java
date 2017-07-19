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

package org.wso2.extension.siddhi.gpl.execution.nlp.utility;

import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.BoolConstant;
import org.wso2.siddhi.query.api.expression.constant.DoubleConstant;
import org.wso2.siddhi.query.api.expression.constant.FloatConstant;
import org.wso2.siddhi.query.api.expression.constant.IntConstant;
import org.wso2.siddhi.query.api.expression.constant.LongConstant;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;

/**
 * Created by malithi on 9/5/14.
 */
public class Constants {
    private Constants() {
    }

    /**
     * Enum for EntityType.
     */
    public enum EntityType {
        PERSON, LOCATION, ORGANIZATION, MONEY, PERCENT, DATE, TIME
    }

    /**
     * Enum for dictionaryTag.
     */
    public enum DictionaryTag {
        ENTITY("entity"),
        ENTRY("entry"),
        ID("id");

        private String tag;

        DictionaryTag(String tag) {
            this.tag = tag;
        }

        public String getTag() {
            return tag;
        }
    }

    public static final String SUBJECT = "subject";
    public static final String OBJECT = "object";
    public static final String VERB = "verb";

    private static final String typeBoolean = "boolean";
    private static final String typeString = "string";
    private static final String typeInt = "int";
    private static final String typeDouble = "double";
    private static final String typeFloat = "float";
    private static final String typeLong = "long";
    private static final String typeVariable = "variable";
    private static final String typeUnknown = "unknown";

    public static String getType(Expression expression) {
        if (expression instanceof BoolConstant) {
            return typeBoolean;
        } else {
            if (expression instanceof StringConstant) {
                return typeString;
            } else if (expression instanceof IntConstant) {
                return typeInt;
            } else if (expression instanceof DoubleConstant) {
                return typeDouble;
            } else if (expression instanceof FloatConstant) {
                return typeFloat;
            } else if (expression instanceof LongConstant) {
                return typeLong;
            } else if (expression instanceof Variable) {
                return typeVariable;
            }
        }

        return typeUnknown;
    }
}
