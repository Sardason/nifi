package org.apache.nifi.processors.caas.split;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;


import static org.apache.nifi.processors.caas.split.AttributeStrategySplitUtil.addCommonSplitAttributes;

public class KeepAllAttributesStrategy implements AttributeSplitStrategy {
    @Override
    public FlowFile updateSplitAttributes(ProcessSession processSession, FlowFile flowFile, String splitId, int splitIndex, String origFileName) {
        return processSession.putAllAttributes(flowFile, addCommonSplitAttributes(flowFile, splitId, splitIndex, origFileName));
    }
}
