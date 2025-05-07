package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.DBMirror;
import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.Environment;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.domain.support.RunStatus;
import com.cloudera.utils.hms.mirror.exceptions.MissingDataPointException;
import com.cloudera.utils.hms.mirror.exceptions.RequiredConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.cloudera.utils.hms.mirror.MirrorConf.SHOW_DATABASES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DatabaseServiceTest {

    private DatabaseService databaseService;

    @Mock
    private ConnectionPoolService connectionPoolService;

    @Mock
    private ExecuteSessionService executeSessionService;

    @Mock
    private ExecuteSession executeSession;

    @Mock
    private HmsMirrorConfig config;

    @Mock
    private ConfigService configService;

    @Mock
    private RunStatus runStatus;

    @Mock
    private QueryDefinitionsService queryDefinitionsService;

    @Mock
    private WarehouseService warehouseService;

    @BeforeEach
    public void setUp() {
        databaseService = new DatabaseService(configService, executeSessionService, connectionPoolService, queryDefinitionsService,
                warehouseService);
        when(executeSessionService.getSession()).thenReturn(executeSession);
        when(executeSession.getConfig()).thenReturn(config);
        when(executeSession.getRunStatus()).thenReturn(runStatus);
    }

    @Test
    public void testListAvailableDatabasesWithResult() throws Exception {
        // Mocking
        Connection mockConnection = mock(Connection.class);
        Statement mockStatement = mock(Statement.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(connectionPoolService.getHS2EnvironmentConnection(Environment.LEFT)).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(SHOW_DATABASES)).thenReturn(mockResultSet);

        when(mockResultSet.next()).thenReturn(true, true, false);
        when(mockResultSet.getString(1)).thenReturn("database1", "database2");

        // Execution
        List<String> databases = databaseService.listAvailableDatabases(Environment.LEFT);

        // Verifications
        verify(connectionPoolService).getHS2EnvironmentConnection(Environment.LEFT);
        verify(mockConnection).createStatement();
        verify(mockStatement).executeQuery(SHOW_DATABASES);
        verify(mockResultSet, times(3)).next();

        // Assertions
        assertThat(databases).containsExactly("database1", "database2");
    }

    @Test
    public void testListAvailableDatabasesWithNoResult() throws Exception {
        // Mocking
        Connection mockConnection = mock(Connection.class);
        Statement mockStatement = mock(Statement.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(connectionPoolService.getHS2EnvironmentConnection(Environment.LEFT)).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(SHOW_DATABASES)).thenReturn(mockResultSet);

        when(mockResultSet.next()).thenReturn(false);

        // Execution
        List<String> databases = databaseService.listAvailableDatabases(Environment.LEFT);

        // Verifications
        verify(connectionPoolService).getHS2EnvironmentConnection(Environment.LEFT);
        verify(mockConnection).createStatement();
        verify(mockStatement).executeQuery(SHOW_DATABASES);
        verify(mockResultSet).next();

        // Assertions
        assertThat(databases).isEmpty();
    }

    @Test
    public void testListAvailableDatabasesSQLException() throws Exception {
        // Mocking
        Connection mockConnection = mock(Connection.class);
        when(connectionPoolService.getHS2EnvironmentConnection(Environment.LEFT)).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenThrow(new SQLException("SQL Exception"));

        // Execution
        List<String> databases = databaseService.listAvailableDatabases(Environment.LEFT);

        // Verifications
        verify(connectionPoolService).getHS2EnvironmentConnection(Environment.LEFT);

        // Assertions
        assertThat(databases).isEmpty();
    }

    @Test
    public void testBuildDBStatementsSuccess() {
        // Mocking
        DBMirror dbMirror = mock(DBMirror.class);
        when(dbMirror.getName()).thenReturn("testDB");

        try {
            when(executeSessionService.getSession().getConfig()).thenReturn(mock(HmsMirrorConfig.class));
            boolean result = databaseService.buildDBStatements(dbMirror);

            // Assertions
            assertThat(result).isTrue();

        } catch (Exception e) {
            fail("No exception expected.");
        }
    }

    @Test
    public void testBuildDBStatementsMissingDataPointException() {
        // Mocking
        DBMirror dbMirror = mock(DBMirror.class);
        when(dbMirror.getName()).thenReturn("testDB");

//        try {
            doThrow(new MissingDataPointException("Missing data point"))
                    .when(executeSessionService.getSession())
                    .getConfig();
            boolean result = databaseService.buildDBStatements(dbMirror);

            // Assertions
            assertThat(result).isFalse();

//        } catch (MissingDataPointException e) {
//             Expected behavior
//            assertThat(e.getMessage()).contains("Missing data point");
//        } catch (Exception e) {
//            fail("No other exception expected.");
//        }
    }

    @Test
    public void testBuildDBStatementsRequiredConfigurationException() {
        // Mocking
        DBMirror dbMirror = mock(DBMirror.class);
        when(dbMirror.getName()).thenReturn("testDB");

//        try {
            doThrow(new RequiredConfigurationException("Required configuration missing"))
                    .when(executeSessionService.getSession())
                    .getConfig();
            boolean result = databaseService.buildDBStatements(dbMirror);

            // Assertions
            assertThat(result).isFalse();

//        } catch (RequiredConfigurationException e) {
            // Expected behavior
//            assertThat(e.getMessage()).contains("Required configuration missing");
//        } catch (Exception e) {
//            fail("No other exception expected.");
//        }
    }
}