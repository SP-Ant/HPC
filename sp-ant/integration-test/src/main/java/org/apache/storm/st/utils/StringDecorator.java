/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.utils;

import org.apache.commons.lang.StringUtils;

/**
 * This class provides a method to pass data from the test bolts and spouts to the test method, via the worker log.
 * Test components can use {@link #decorate(java.lang.String, java.lang.String) } to create a string containing a
 * unique prefix.
 * Such prefixed log lines can be retrieved from the worker logs, and recognized via
 * {@link #isDecorated(java.lang.String, java.lang.String) }.
 */
public class StringDecorator {

    private static final String UNIQUE_PREFIX = "---bed91874d79720f7e324c43d49dba4ff---";

    public static String decorate(String componentId, String decorate) {
        return componentId + UNIQUE_PREFIX + decorate;
    }

    public static boolean isDecorated(String componentId, String str) {
        return str != null && str.contains(componentId + UNIQUE_PREFIX);
    }

    public static String[] split2(String decoratedString) {
        return StringUtils.splitByWholeSeparator(decoratedString, UNIQUE_PREFIX, 2);
    }
}
