package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.datastrategy.*;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.DataStrategyEnum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

//@ExtendWith(SpringExtension.class)
//@SpringBootTest(classes = MockMirror.class,
//        args = {
//                "--hms-mirror.config.filename=/config/default.yaml.hdp2-cdp"
//        })
@ExtendWith(MockitoExtension.class)
public class DataStrategyServiceTest {

    @Mock
    private CommonDataStrategy commonDataStrategy;

    @Mock
    private ConvertLinkedDataStrategy convertLinkedDataStrategy;

    @Mock
    private DumpDataStrategy dumpDataStrategy;

    @Mock
    private ExportImportAcidDowngradeInPlaceDataStrategy exportImportAcidDowngradeInPlaceDataStrategy;

    @Mock
    private ExportImportDataStrategy exportImportDataStrategy;

    @Mock
    private HybridDataStrategy hybridDataStrategy;

    @Mock
    private HybridAcidDowngradeInPlaceDataStrategy hybridAcidDowngradeInPlaceDataStrategy;

    @Mock
    private LinkedDataStrategy linkedDataStrategy;

    @Mock
    private SchemaOnlyDataStrategy schemaOnlyDataStrategy;

    @Mock
    private StorageMigrationDataStrategy storageMigrationDataStrategy;

    @Mock
    private SQLDataStrategy sqlDataStrategy;

    @Mock
    private SQLAcidInPlaceDataStrategy sqlAcidInPlaceDataStrategy;

    @Mock
    private IntermediateDataStrategy intermediateDataStrategy;

    @Mock
    private IcebergConversionDataStrategy icebergConversionDataStrategy;

    @Mock
    private AcidDataStrategy acidDataStrategy;

    @InjectMocks
    private DataStrategyService dataStrategyService;

    @Test
    public void testGetDefaultDataStrategy_ReturnsDefaultStrategyWhenNoStrategyInConfig() {
        HmsMirrorConfig config = Mockito.mock(HmsMirrorConfig.class);
        Mockito.when(config.getDataStrategy()).thenReturn(DataStrategyEnum.SCHEMA_ONLY);

        DataStrategy result = dataStrategyService.getDefaultDataStrategy(config);

        assertEquals(schemaOnlyDataStrategy, result, "Expected default strategy to be returned.");
    }

    @Test
    public void testGetDefaultDataStrategy_ReturnsCorrectStrategyForStorageMigration() {
        HmsMirrorConfig config = Mockito.mock(HmsMirrorConfig.class);
        Mockito.when(config.getDataStrategy()).thenReturn(DataStrategyEnum.STORAGE_MIGRATION);

        DataStrategy result = dataStrategyService.getDefaultDataStrategy(config);

        assertEquals(storageMigrationDataStrategy, result, "Expected STORAGE_MIGRATION strategy to be returned.");
    }

    @Test
    public void testGetDefaultDataStrategy_ReturnsCorrectStrategyForIcebergConversion() {
        HmsMirrorConfig config = Mockito.mock(HmsMirrorConfig.class);
        Mockito.when(config.getDataStrategy()).thenReturn(DataStrategyEnum.ICEBERG_CONVERSION);

        DataStrategy result = dataStrategyService.getDefaultDataStrategy(config);

        assertEquals(icebergConversionDataStrategy, result, "Expected ICEBERG_CONVERSION strategy to be returned.");
    }

    @Test
    public void testGetDefaultDataStrategy_ReturnsCorrectStrategyForDump() {
        HmsMirrorConfig config = Mockito.mock(HmsMirrorConfig.class);
        Mockito.when(config.getDataStrategy()).thenReturn(DataStrategyEnum.DUMP);

        DataStrategy result = dataStrategyService.getDefaultDataStrategy(config);

        assertEquals(dumpDataStrategy, result, "Expected DUMP strategy to be returned.");
    }

    @Test
    public void testGetDefaultDataStrategy_ReturnsCorrectStrategyForExportImport() {
        HmsMirrorConfig config = Mockito.mock(HmsMirrorConfig.class);
        Mockito.when(config.getDataStrategy()).thenReturn(DataStrategyEnum.EXPORT_IMPORT);

        DataStrategy result = dataStrategyService.getDefaultDataStrategy(config);

        assertEquals(exportImportDataStrategy, result, "Expected EXPORT_IMPORT strategy to be returned.");
    }

    @Test
    public void testGetDefaultDataStrategy_ReturnsCorrectStrategyForHybrid() {
        HmsMirrorConfig config = Mockito.mock(HmsMirrorConfig.class);
        Mockito.when(config.getDataStrategy()).thenReturn(DataStrategyEnum.HYBRID);

        DataStrategy result = dataStrategyService.getDefaultDataStrategy(config);

        assertEquals(hybridDataStrategy, result, "Expected HYBRID strategy to be returned.");
    }

    @Test
    public void testGetDefaultDataStrategy_ReturnsCorrectStrategyForUnknownStrategy() {
        HmsMirrorConfig config = Mockito.mock(HmsMirrorConfig.class);
        Mockito.when(config.getDataStrategy()).thenReturn(DataStrategyEnum.COMMON);

        DataStrategy result = dataStrategyService.getDefaultDataStrategy(config);

        assertEquals(commonDataStrategy, result, "Expected COMMON strategy to be returned.");
    }
}