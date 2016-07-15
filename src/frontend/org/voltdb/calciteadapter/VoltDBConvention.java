package org.voltdb.calciteadapter;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;

enum VoltDBConvention implements Convention {
    INSTANCE;

    /** Cost of an VoltDB node versus implementing an equivalent node in a
     * "typical" calling convention. */
    public static final double COST_MULTIPLIER = 1.0d;

    @Override public String toString() {
      return getName();
    }

    @Override
    public Class getInterface() {
      return VoltDBRel.class;
    }

    @Override
    public String getName() {
      return "VOLTDB";
    }

    @Override
    public RelTraitDef getTraitDef() {
      return ConventionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
      return this == trait;
    }

    @Override
    public void register(RelOptPlanner planner) {}

    @Override
    public boolean canConvertConvention(Convention toConvention) {
      return false;
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
        RelTraitSet toTraits) {
      return false;
    }
  }
