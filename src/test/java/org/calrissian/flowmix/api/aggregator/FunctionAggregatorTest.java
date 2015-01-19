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

import java.util.ArrayList;
import java.util.List;
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
public class FunctionAggregatorTest {

    public FunctionAggregatorTest() {
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
        //Wanted to use standard deviation for testing this Aggregator, is a very heavy operation
        System.out.println("FunctionAggregatorTest");
        FunctionAggregator<Double, Long> instance = new FunctionAggregator<Double, Long>("stnd", new FunctionAggregator.AggregatorFunction<Double, Long>() {

            private static final String VALUES = "values";
            private static final String MEAN = "mean";

            private Double calculateAverage(List<Long> marks) {
                Long sum = (long) 0;
                if (!marks.isEmpty()) {
                    for (Long mark : marks) {
                        sum += mark;
                    }
                    return sum.doubleValue() / marks.size();
                }
                return (double) sum;
            }

            @Override
            public void add(Long value) {
                if (value == null || !getData().containsKey(VALUES)) {
                    getData().put(VALUES, new ArrayList<Long>());
                }
                ((ArrayList<Long>) getData().get(VALUES)).add(value);
                getData().put(MEAN, calculateAverage((ArrayList<Long>) getData().get(VALUES)));
            }

            @Override
            public void evict(Long value) {
                if (value == null || !getData().containsKey(VALUES) || ((ArrayList<Long>) getData().get(VALUES)).isEmpty()) {
                    return;
                }
                ((ArrayList<Long>) getData().get(VALUES)).remove(value);
                getData().put(MEAN, calculateAverage((ArrayList<Long>) getData().get(VALUES)));
            }

            @Override
            public Double aggregate() {
                Long[] sq = new Long[((ArrayList<Long>) getData().get(VALUES)).size()];
                ((ArrayList<Long>) getData().get(VALUES)).toArray(sq);
                Double sum = 0.0;
                for (Long e : sq) {
                    Double e2 = (e - (Double) getData().get(MEAN));
                    sum = sum + (e2 * e2);
                }
                return Math.sqrt((sum / sq.length));
            }
        });
        instance.postAddition((long) 1);
        instance.postAddition((long) 1);
        instance.postAddition((long) 20);
        instance.postAddition((long) 5);
        instance.postAddition((long) -3);
        instance.evict((long) 1);
        Double result = instance.aggregateResult();
        Double expResult = 8.699856320652657;
        assertEquals(expResult, result);
    }

}
