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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class AvgAggregatorTest {

    public AvgAggregatorTest() {
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
    public void test() {
        System.out.println("AvgAggregatorTest");
        AvgAggregator instance = new AvgAggregator();
        instance.add((long) 1);
        instance.add((long) 10);
        instance.add((long) -5);
        instance.evict((long) 1);
        Double result = instance.aggregateResult();
        Double expectedResult = 2.5;
        assertEquals(expectedResult, result);
    }

        
    @Test
    public void testNoItems() {
        System.out.println("AvgAggregatorTest - No Items");
        AvgAggregator instance = new AvgAggregator();
        instance.add((long)33);
        instance.add((long)33);
        instance.add((long)33);
        instance.add((long)33);
        instance.evict((long)33);
        instance.evict((long)33);
        instance.evict((long)33);
        instance.evict((long)33);
        Double result = instance.aggregateResult();
        Double expectedResult = (double)0.0;
        assertEquals(expectedResult, result);
    }

    
}
