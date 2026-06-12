package org.apache.nifi.processors.caas.split;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.StringUtils;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.caas.merge.KeepAllAttributeStrategy.MERGE_ATTRIBUTES_ATTRIBUTE;
import static org.apache.nifi.processors.caas.split.AttributeStrategySplitUtil.addCommonSplitAttributes;

public class KeepByIndexAttributeStrategy implements AttributeSplitStrategy {
    @Override
    public FlowFile updateSplitAttributes(ProcessSession processSession, FlowFile flowFile, String splitId, int splitIndex, String origFileName) {
        flowFile = processSession.putAllAttributes(flowFile, addCommonSplitAttributes(flowFile, splitId, splitIndex, origFileName));

        String mergedAttributes = flowFile.getAttribute(MERGE_ATTRIBUTES_ATTRIBUTE);
        if (StringUtils.isEmpty(mergedAttributes)) return flowFile;

        ObjectMapper mapper = new ObjectMapper();

        try {
            Map<String, Map<String, String>> mergedAttributesDict = mapper.readValue(mergedAttributes, new TypeReference<>() {
            });

            flowFile = processSession.putAllAttributes(flowFile, mergedAttributesDict.get(String.valueOf(splitIndex)));
            return processSession.removeAttribute(flowFile, MERGE_ATTRIBUTES_ATTRIBUTE);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}


