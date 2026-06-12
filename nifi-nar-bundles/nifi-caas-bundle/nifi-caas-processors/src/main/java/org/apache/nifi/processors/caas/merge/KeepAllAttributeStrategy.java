package org.apache.nifi.processors.caas.merge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.util.*;

public class KeepAllAttributeStrategy implements AttributeStrategy {
    public static final String MERGE_ATTRIBUTES_ATTRIBUTE = "merge.attributes";

    @Override
    public Map<String, String> getMergedAttributes(final List<FlowFile> flowFiles) {
        final Map<String, String> newAttributes = new HashMap<>();
        final Map<String, Map<String, String>> mergedAttributes = new HashMap<>();

        int flowFileIndex = 0;


        for (final FlowFile flowFile : flowFiles) {
            Map<String, String> allAttributes = flowFile.getAttributes();
            mergedAttributes.put(String.valueOf(flowFileIndex), allAttributes);
            flowFileIndex++;
        }

        try {
            newAttributes.put(MERGE_ATTRIBUTES_ATTRIBUTE, new ObjectMapper().writeValueAsString(mergedAttributes));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        // Never copy the UUID from the parents - which could happen if we don't remove it and there is only 1 parent.
        newAttributes.remove(CoreAttributes.UUID.key());
        return newAttributes;
    }
}
