package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Objects;

/**
 * CREATE AGGREGATE name (argtype [, ...]) (
 *     SFUNC = sfunc, STYPE = stype [, INITCOND = initcond] [, FINALFUNC = finalfunc] ...
 * )
 */
public final class CreateAggregateStmt implements Statement {
    public final String name;
    public final List<String> argTypes;
    public final String sfunc;
    public final String stype;
    public final String initcond;    // may be null
    public final String finalfunc;   // may be null
    public final String combinefunc; // may be null
    public final String sortop;      // may be null

    public CreateAggregateStmt(String name, List<String> argTypes, String sfunc, String stype,
                               String initcond, String finalfunc, String combinefunc, String sortop) {
        this.name = name;
        this.argTypes = argTypes;
        this.sfunc = sfunc;
        this.stype = stype;
        this.initcond = initcond;
        this.finalfunc = finalfunc;
        this.combinefunc = combinefunc;
        this.sortop = sortop;
    }

    public String name() { return name; }
    public List<String> argTypes() { return argTypes; }
    public String sfunc() { return sfunc; }
    public String stype() { return stype; }
    public String initcond() { return initcond; }
    public String finalfunc() { return finalfunc; }
    public String combinefunc() { return combinefunc; }
    public String sortop() { return sortop; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateAggregateStmt that = (CreateAggregateStmt) o;
        return Objects.equals(name, that.name)
            && Objects.equals(argTypes, that.argTypes)
            && Objects.equals(sfunc, that.sfunc)
            && Objects.equals(stype, that.stype)
            && Objects.equals(initcond, that.initcond)
            && Objects.equals(finalfunc, that.finalfunc)
            && Objects.equals(combinefunc, that.combinefunc)
            && Objects.equals(sortop, that.sortop);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, argTypes, sfunc, stype, initcond, finalfunc, combinefunc, sortop);
    }

    @Override
    public String toString() {
        return "CreateAggregateStmt[name=" + name + ", argTypes=" + argTypes + ", sfunc=" + sfunc
            + ", stype=" + stype + ", initcond=" + initcond + ", finalfunc=" + finalfunc + "]";
    }
}
