package de.hpi.rdse.jujo.startup;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

import java.io.File;

public class DirectoryValidator implements IValueValidator<String> {

    @Override
    public void validate(String name, String value) throws ParameterException {
        File file = new File(value);
        if (!file.exists()) {
            file.mkdirs();
        }

        if (!file.isDirectory()) {
            throw new ParameterException("the given " + name + " is not a file");
        }
    }
}
