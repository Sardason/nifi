package org.apache.nifi.processors.caas.split;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.ProcessContext;

import java.util.HashMap;
import java.util.Map;


public class AttributeStrategySplitUtil {

    public static final AllowableValue ATTRIBUTE_STRATEGY_KEEP_ALL_ATTRIBUTES = new AllowableValue("Keep All Attributes", "Keep All Attributes",
            "Will have the same attributes as before the split");
    public static final AllowableValue ATTRIBUTE_STRATEGY_BY_INDEX = new AllowableValue("Restore Merged Attributes", "Restore Merged Attributes",
            "If the Flowfile is a merged flowfile and it used with Keep All Attribute Strategy, this strategy will restore the flowfile header by the index of the attribute");


    public static final PropertyDescriptor ATTRIBUTE_STRATEGY = new PropertyDescriptor.Builder()
            .required(true)
            .name("Attribute Strategy")
            .description("Determines which FlowFile attributes should be added to the bundle. If 'Keep All Attributes' is selected, any "
                    + "attribute on any FlowFile that gets bundled will be kept unless its value conflicts with the value from another FlowFile. "
                    + "If 'Keep By Index Attributes' is selected, each flowfile that was splitted will have all the attributes of the fitting index - used only after flowfile was merged")
            .allowableValues(ATTRIBUTE_STRATEGY_KEEP_ALL_ATTRIBUTES, ATTRIBUTE_STRATEGY_BY_INDEX)
            .defaultValue(ATTRIBUTE_STRATEGY_KEEP_ALL_ATTRIBUTES.getValue())
            .build();


    public static AttributeSplitStrategy strategyFor(ProcessContext context) {
        final String strategyName = context.getProperty(ATTRIBUTE_STRATEGY).getValue();
        if (ATTRIBUTE_STRATEGY_KEEP_ALL_ATTRIBUTES.getValue().equals(strategyName)) {
            return new KeepAllAttributesStrategy();
        }
        if (ATTRIBUTE_STRATEGY_BY_INDEX.getValue().equals(strategyName)) {
            return new KeepByIndexAttributeStrategy();
        }

        return null;
    }

    public static Map<String, String> addCommonSplitAttributes(FlowFile splitFlowFile,
                                                         String splitId, int splitIndex, String origFileName) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(FragmentAttributes.FRAGMENT_SIZE.key(), String.valueOf(splitFlowFile.getSize()));
        attributes.put(FragmentAttributes.FRAGMENT_ID.key(), splitId);
        attributes.put(FragmentAttributes.FRAGMENT_INDEX.key(), String.valueOf(splitIndex));
        attributes.put(FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key(), origFileName);

        return attributes;
    }
}
