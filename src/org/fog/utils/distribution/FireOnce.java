package org.fog.utils.distribution;

import static org.fog.utils.Config.MAX_SIMULATION_TIME;

public class FireOnce extends Distribution{

    private double value;
    private boolean fired = false;

    public FireOnce(double value) {
        super();
        setValue(value);
    }

    @Override
    public double getNextValue() {
        if (fired) return MAX_SIMULATION_TIME + 10000;
        fired = true;
        return value;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public int getDistributionType() {
        return Distribution.DETERMINISTIC;
    }

    @Override
    public double getMeanInterTransmitTime() {
        return value;
    }

}
