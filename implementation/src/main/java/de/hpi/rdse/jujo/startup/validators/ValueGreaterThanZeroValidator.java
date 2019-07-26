package de.hpi.rdse.jujo.startup.validators;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

public class ValueGreaterThanZeroValidator implements IValueValidator<Number> {

    @Override
    public void validate(String valueName, Number value) throws ParameterException {
        if (value.doubleValue() > 0.0d) {
            return;
        }

        throw new ParameterException(String.format("%s must be greater than 0!", valueName));
    }
}
