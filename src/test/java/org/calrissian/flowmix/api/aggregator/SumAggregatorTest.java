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

/**
 * @author Miguel A. Fuentes Buchholtz
 */
public class SumAggregatorTest {
    
    public SumAggregatorTest() {
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
        System.out.println("SumAggregatorTest");
        SumAggregator instance = new SumAggregator();
        instance.postAddition((long) 1);
        instance.postAddition((long) 1);
        instance.postAddition((long) 1);
        instance.postAddition((long) 3);
        instance.evict((long)3);
        instance.postAddition((long) 1);
        Long result = instance.aggregateResult();
        Long expectedResult = (long)4;
        assertEquals(expectedResult, result);
    }

}
