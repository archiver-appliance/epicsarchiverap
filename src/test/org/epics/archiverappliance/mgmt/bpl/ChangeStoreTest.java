package org.epics.archiverappliance.mgmt.bpl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

class ChangeStoreTest {

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
        when(request.getParameter("newbackend")).thenReturn("testBackend");
        when(request.getParameter("storage")).thenReturn(null);
        changeStore.execute(request, response, configService);
        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);

        // Missing newbackend
        reset(response);
        when(request.getParameter("pv")).thenReturn("testPV");
        when(request.getParameter("storage")).thenReturn("testStorage");
        when(request.getParameter("newbackend")).thenReturn(null);
        changeStore.execute(request, response, configService);
        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);
    }

    @Test
    void testExecutePVNotPaused() throws IOException {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ConfigService configService = mock(ConfigService.class);
        ApplianceInfo applianceInfo = mock(ApplianceInfo.class);
        PVTypeInfo pvTypeInfo = mock(PVTypeInfo.class);

        String pvName = "testPV";
        String storageName = "testStorage";
        String newBackend = "pb://localhost?name=newStorage&rootFolder=/tmp/newStorage";

        when(request.getParameter("pv")).thenReturn(pvName);
        when(request.getParameter("storage")).thenReturn(storageName);
        when(request.getParameter("newbackend")).thenReturn(newBackend);

        when(configService.getRealNameForAlias(pvName)).thenReturn(null);
        when(configService.getApplianceForPV(pvName)).thenReturn(applianceInfo);
        when(configService.getMyApplianceInfo()).thenReturn(applianceInfo);
        when(applianceInfo.getIdentity()).thenReturn("thisAppliance");
        when(configService.getTypeInfoForPV(pvName)).thenReturn(pvTypeInfo);
        when(pvTypeInfo.isPaused()).thenReturn(false); // Not paused

        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);

        ChangeStore changeStore = new ChangeStore();
        changeStore.execute(request, response, configService);

        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(stringWriter.toString().contains("Need to pause PV"));
    }

    @Test
    void testExecuteStoragePluginNotFound() throws IOException {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ConfigService configService = mock(ConfigService.class);
        ApplianceInfo applianceInfo = mock(ApplianceInfo.class);
        PVTypeInfo pvTypeInfo = mock(PVTypeInfo.class);

        String pvName = "testPV";
        String storageName = "testStorage";
        String newBackend = "pb://localhost?name=newStorage&rootFolder=/tmp/newStorage";

        when(request.getParameter("pv")).thenReturn(pvName);
        when(request.getParameter("storage")).thenReturn(storageName);
        when(request.getParameter("newbackend")).thenReturn(newBackend);

        when(configService.getRealNameForAlias(pvName)).thenReturn(null);
        when(configService.getApplianceForPV(pvName)).thenReturn(applianceInfo);
        when(configService.getMyApplianceInfo()).thenReturn(applianceInfo);
        when(applianceInfo.getIdentity()).thenReturn("thisAppliance");
        when(configService.getTypeInfoForPV(pvName)).thenReturn(pvTypeInfo);
        when(pvTypeInfo.isPaused()).thenReturn(true); // Paused
        when(pvTypeInfo.getDataStores()).thenReturn(new String[] {}); // No stores

        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);

        ChangeStore changeStore = new ChangeStore();
        changeStore.execute(request, response, configService);

        verify(response).sendError(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(stringWriter.toString().contains("Cannot find storage with name"));
    }

    @Test
    void testExecuteProxy() throws IOException {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        ConfigService configService = mock(ConfigService.class);
        ApplianceInfo applianceInfo = mock(ApplianceInfo.class);
        ApplianceInfo myApplianceInfo = mock(ApplianceInfo.class);

        String pvName = "testPV";
        String storageName = "testStorage";
        String newBackend = "pb://localhost?name=newStorage&rootFolder=/tmp/newStorage";

        when(request.getParameter("pv")).thenReturn(pvName);
        when(request.getParameter("storage")).thenReturn(storageName);
        when(request.getParameter("newbackend")).thenReturn(newBackend);

        when(configService.getRealNameForAlias(pvName)).thenReturn(null);
        when(configService.getApplianceForPV(pvName)).thenReturn(applianceInfo);
        when(configService.getMyApplianceInfo()).thenReturn(myApplianceInfo);

        when(applianceInfo.getIdentity()).thenReturn("otherAppliance");
        when(myApplianceInfo.getIdentity()).thenReturn("thisAppliance");
        when(applianceInfo.getMgmtURL()).thenReturn("http://other:17665/mgmt/bpl");

        StringWriter stringWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("status", "ok");

        try (MockedStatic<GetUrlContent> getUrlContentMock = mockStatic(GetUrlContent.class)) {
            getUrlContentMock
                    .when(() -> GetUrlContent.getURLContentAsJSONObject(anyString()))
                    .thenReturn(jsonObject);

            ChangeStore changeStore = new ChangeStore();
            changeStore.execute(request, response, configService);

            verify(response).setContentType(MimeTypeConstants.APPLICATION_JSON);
            assertTrue(stringWriter.toString().contains("{\"status\":\"ok\"}"));
        }
    }
}
