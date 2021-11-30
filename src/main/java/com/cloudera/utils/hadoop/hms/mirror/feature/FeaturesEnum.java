package com.cloudera.utils.hadoop.hms.mirror.feature;

public enum FeaturesEnum {
    BAD_FIELDS_FORM_FEED_DEF(BadFieldsFFDefFeature.class),
    BAD_ORC_DEF(BadOrcDefFeature.class),
    BAD_RC_DEF(BadRCDefFeature.class),
    BAD_PARQUET_DEF(BadParquetDefFeature.class),
    BAD_TEXTFILE_DEF(BadTextFileDefFeature.class);

    private Feature feature;

    public Feature getFeature() {
        return feature;
    }

    private FeaturesEnum(Class featureClass) {
        try {
            feature = (Feature)featureClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
