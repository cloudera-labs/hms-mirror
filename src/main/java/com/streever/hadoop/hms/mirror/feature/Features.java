package com.streever.hadoop.hms.mirror.feature;

public enum Features {
    BAD_ORC_DEF(BadOrcDefFeature.class),
    BAD_RC_DEF(BadRCDefFeature.class),
    BAD_TEXTFILE_DEF(BadTextFileDefFeature.class);

    private Feature feature;

    public Feature getFeature() {
        return feature;
    }

    private Features(Class featureClass) {
        try {
            feature = (Feature)featureClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
