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

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 *
 * Awesome Javascript Function Aggregator. This is going to move somewhere over the rainbow
 * 
 * @author Miguel A. Fuentes Buchholtz
 * @param <T> Aggregation result type
 * @param <F> Field type
 */
@Deprecated
public class JavascriptFunctionAggregator<T,F> extends AbstractGrouplessAggregator<T,F> {

    private final ScriptEngineManager manager = new ScriptEngineManager();
    private final ScriptEngine engine = manager.getEngineByName("js");
    private final Invocable invocable;
    
    //This is optional, but I think it's simpler for someone trying to focus on the Function implementation
    //by this class not being Abstract
    /*
    * @param javascript has to have an implementation of postAddition(F fieldValue), evict(F fieldValue) and T aggregateResult()
    */
    public JavascriptFunctionAggregator(String outputField, String javascript) throws ScriptException {
        super();
        super.outputField = outputField;
        engine.eval(javascript);
        this.invocable = (Invocable) engine;
    }

    @Override
    protected String getOutputField() {
        return null;
    }

    @Override
    public void postAddition(F fieldValue) {
        try {
            this.invocable.invokeFunction("postAddition", fieldValue );
        } catch (ScriptException ex) {
            Logger.getLogger(JavascriptFunctionAggregator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(JavascriptFunctionAggregator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void evict(F fieldValue) {
        try {
            this.invocable.invokeFunction("fieldValue", fieldValue);
        } catch (ScriptException ex) {
            Logger.getLogger(JavascriptFunctionAggregator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(JavascriptFunctionAggregator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    protected T aggregateResult() {
        try {
            return (T) this.invocable.invokeFunction("aggregateResult", (new Object[0]));
        } catch (ScriptException ex) {
            Logger.getLogger(JavascriptFunctionAggregator.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(JavascriptFunctionAggregator.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
}
