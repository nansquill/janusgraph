package org.janusgraph.blueprints.process;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.lang.reflect.Field;

public class FDBProcessStandardSuite extends ProcessStandardSuite {

    public FDBProcessStandardSuite(final Class<?> classToTest, final RunnerBuilder builder) throws InitializationError {
        super(classToTest, builder, getTestList());
    }

    private static Class<?>[] getTestList() throws InitializationError {
        try {
            final Field field = ProcessStandardSuite.class.getDeclaredField("allTests");
            field.setAccessible(true);
            return (Class<?>[]) ArrayUtils.removeElement((Class<?>[]) field.get(null), TraversalInterruptionTest.class);
        } catch (ReflectiveOperationException e) {
            throw new InitializationError("Unable to create test list");
        }
    }
}
