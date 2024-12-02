package org.apache.wayang.java.mapping;

import java.util.Collection;
import java.util.Collections;

import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.java.operators.JavaParquetFileSource;
import org.apache.wayang.java.platform.JavaPlatform;

public class ParquetFileSourceMapping implements Mapping {

@Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new org.apache.wayang.basic.operators.ParquetFileSource((String) null), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ParquetFileSource>(
                (matchedOperator, epoch) -> new JavaParquetFileSource(matchedOperator).at(epoch)
        );
    }


}
