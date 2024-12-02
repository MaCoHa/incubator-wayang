package org.apache.wayang.basic.operators;

import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

public class ParquetFileSource extends UnarySource<GenericRecord> {
   
  
    private final Logger logger = LogManager.getLogger(this.getClass());

    private final String inputUrl;


    public ParquetFileSource(String inputUrl) {
        super(DataSetType.createDefault(GenericRecord.class));
        this.inputUrl = inputUrl;

    }
    public ParquetFileSource(ParquetFileSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
    }

    public String getInputUrl() {
        return this.inputUrl;
    }

}
