/**
 * Copyright (C) 2011 apendr.com
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 *  _  _  _  _  _  _| _
 * (_||_)|_)(/_| |(_||
 *    |  |
 */
package com.appendr.streamd.cep;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Subscriber {

    /**
     * Associate a subscriber with an Esper Statement(s)
     *
     * @return Name of the subscriber implementation
     */
    public String value();
}
