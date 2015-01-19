/*
 * Copyright 2015 Calrissian.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.api.aggregator;

import javax.script.ScriptException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author miguel
 */
public class JavascriptFunctionAggregatorTest {
    
    public JavascriptFunctionAggregatorTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testGetOutputField() throws ScriptException {
        System.out.println("JavascriptFunctionAggregatorTest");
        String js = "var sum = 0;"
                + "postAddition = function(fieldValue){ sum = sum + fieldValue; };"
                + "evict = function(fieldValue){ sum = sum - fieldValue; };"
                + "aggregateResult = function(){ return sum; }";
        JavascriptFunctionAggregator<Double,Long> instance = new JavascriptFunctionAggregator<Double,Long>("stnd", js);
        instance.postAddition((long) 1);
        instance.postAddition((long) 1);
        instance.postAddition((long) 1);
        instance.postAddition((long) 3);
        instance.evict((long)3);
        instance.postAddition((long) 1);
        Double result = instance.aggregateResult();
        long expectedResult = (long)7;
        assertEquals(expectedResult, result.longValue());
    }

}
