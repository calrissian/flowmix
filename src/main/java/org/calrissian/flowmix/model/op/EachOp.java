/*
 * Copyright (C) 2014 The Calrissian Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.calrissian.flowmix.model.op;

import org.calrissian.flowmix.support.Function;

public class EachOp implements FlowOp {

    public static final String EACH = "each";

    Function function;

    public EachOp(Function function) {
        this.function = function;
    }

    public Function getFunction() {
        return function;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EachOp that = (EachOp) o;

        if (function != null ? !function.equals(that.function) : that.function != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return function != null ? function.hashCode() : 0;
    }

    @Override
    public String getComponentName() {
        return "each";
    }
}
