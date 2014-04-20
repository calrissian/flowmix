package org.calrissian.flowbox.model;

import org.calrissian.flowbox.support.Function;

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
