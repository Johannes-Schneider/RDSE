package de.hpi.rdse.jujo.startup;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

import java.io.File;
import java.nio.file.Path;

public class FileValidator implements IValueValidator<Path> {

    @Override
    public void validate(String name, Path value) throws ParameterException {
        File file = value.toFile();
        if (!file.exists()) {
            throw new ParameterException("the given " + name + " does not exist");
        }

        if (!file.isFile()) {
            throw new ParameterException("the given " + name + " is not a file");
        }
    }
}
