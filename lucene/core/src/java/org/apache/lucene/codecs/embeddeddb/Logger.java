package org.apache.lucene.codecs.embeddeddb;

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

import java.util.Date;

/**
 * Created by rlmathes on 7/22/17.
 *
 * Skeleton class for future simple plain Java logger
 *
 */
public class Logger {

    public static void error(final String message) {
        Date currentDate = new Date(System.currentTimeMillis());
        System.out.println(currentDate.toString() + " ERROR message: " + message);
    }

    public static void warn(final String message) {
        Date currentDate = new Date(System.currentTimeMillis());
        System.out.println(currentDate.toString() + " WARN message: " + message);
    }

    public static void info(final String message) {
        Date currentDate = new Date(System.currentTimeMillis());
        System.out.println(currentDate.toString() + " INFO message: " + message);
    }
    public static void debug(final String message) {
        Date currentDate = new Date(System.currentTimeMillis());
        System.out.println(currentDate.toString() + " DEBUG message: " + message);
    }

    public static void trace(final String message) {
        Date currentDate = new Date(System.currentTimeMillis());
        System.out.println(currentDate.toString() + " TRACE message: " + message);
    }

}
