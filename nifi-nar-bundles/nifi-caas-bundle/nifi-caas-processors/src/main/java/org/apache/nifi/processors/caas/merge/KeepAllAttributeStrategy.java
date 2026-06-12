package org.apache.nifi.processors.caas.merge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.util.*;

public class KeepAllAttributeStrategy implements AttributeStrategy {
    @Override
    public Map<String, String> getMergedAttributes(final List<FlowFile> flowFiles) {
        final Map<String, String> newAttributes = new HashMap<>();

        int flowFileIndex = 0;

        for (final FlowFile flowFile : flowFiles) {
            final Map<String, String> allAttributes = flowFile.getAttributes();
            try {
                String jsonAttributes = new ObjectMapper().writeValueAsString(allAttributes);
                newAttributes.put(String.valueOf(flowFileIndex), jsonAttributes);
            } catch (JsonProcessingException e) {
                newAttributes.put(String.valueOf(flowFileIndex), "no attributes");
            }

            flowFileIndex++;
        }

        // Never copy the UUID from the parents - which could happen if we don't remove it and there is only 1 parent.
        newAttributes.remove(CoreAttributes.UUID.key());
        return newAttributes;
    }
}
