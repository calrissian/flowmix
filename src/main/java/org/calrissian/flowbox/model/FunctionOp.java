package org.calrissian.flowbox.model;

import org.calrissian.flowbox.support.Function;

public class FunctionOp implements FlowOp {

    public static final String FUNCTION = "function";

    Function function;

    public FunctionOp(Function function) {
        this.function = function;
    }

    public Function getFunction() {
        return function;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FunctionOp that = (FunctionOp) o;

        if (function != null ? !function.equals(that.function) : that.function != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return function != null ? function.hashCode() : 0;
    }

    @Override
    public String getComponentName() {
        return "function";
    }
}
