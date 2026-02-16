package org.epics.archiverappliance.etl.bpl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.ETLExecutor;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

class ChangeStoreTest {

    @Test
    void testExecute() throws IOException {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ConfigService configService = mock(ConfigService.class);

        String pvName = "testPV";
        String storageName = "newStorage";
        String newBackend = "pb://localhost?name=newStorage&rootFolder=/tmp/newStorage";

        when(request.getParameter("pv")).thenReturn(pvName);
        when(request.getParameter("storage")).thenReturn(storageName);
        when(request.getParameter("newbackend")).thenReturn(newBackend);

        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);

        try (MockedStatic<ETLExecutor> etlExecutorMock = mockStatic(ETLExecutor.class)) {
            ChangeStore changeStore = new ChangeStore();
            changeStore.execute(request, response, configService);

            etlExecutorMock.verify(
                    () -> ETLExecutor.moveDataFromOneStorageToAnother(configService, pvName, storageName, newBackend));
            String responseString = stringWriter.toString();
            assertTrue(responseString.contains("Successfully changed the storage for PV"));
        }
    }

    @Test
    void testExecuteMissingParameters() throws IOException {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ConfigService configService = mock(ConfigService.class);

        ChangeStore changeStore = new ChangeStore();

        // Missing pv
        when(request.getParameter("pv")).thenReturn(null);
        changeStore.execute(request, response, configService);
        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);

        // Missing storage
        reset(response);
        when(request.getParameter("pv")).thenReturn("testPV");
        when(request.getParameter("storage")).thenReturn(null);
        changeStore.execute(request, response, configService);
        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);

        // Missing newbackend
        reset(response);
        when(request.getParameter("storage")).thenReturn("testStorage");
        when(request.getParameter("newbackend")).thenReturn(null);
        changeStore.execute(request, response, configService);
        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);
    }

    @Test
    void testExecuteException() throws IOException {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ConfigService configService = mock(ConfigService.class);

        String pvName = "testPV";
        String storageName = "testStorage";
        String newBackend = "pb://localhost?name=newStorage&rootFolder=/tmp/newStorage";

        when(request.getParameter("pv")).thenReturn(pvName);
        when(request.getParameter("storage")).thenReturn(storageName);
        when(request.getParameter("newbackend")).thenReturn(newBackend);

        try (MockedStatic<ETLExecutor> etlExecutorMock = mockStatic(ETLExecutor.class)) {
            etlExecutorMock
                    .when(() ->
                            ETLExecutor.moveDataFromOneStorageToAnother(configService, pvName, storageName, newBackend))
                    .thenThrow(new IOException("Test Exception"));

            ChangeStore changeStore = new ChangeStore();
            changeStore.execute(request, response, configService);

            verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }
}
