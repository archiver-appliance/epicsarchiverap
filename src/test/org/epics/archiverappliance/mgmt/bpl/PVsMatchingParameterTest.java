package org.epics.archiverappliance.mgmt.bpl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;

class PVsMatchingParameterTest {

    class DelegatingServletInputStream extends ServletInputStream {

        private final InputStream sourceStream;

        /**
         * Create a DelegatingServletInputStream for the given source stream.
         * @param sourceStream the source stream (never <code>null</code>)
         */
        public DelegatingServletInputStream(InputStream sourceStream) {
            this.sourceStream = sourceStream;
        }

        public int read() throws IOException {
            return this.sourceStream.read();
        }

        @Override
        public void close() throws IOException {
            super.close();
            this.sourceStream.close();
        }

        @Override
        public boolean isFinished() {
            return this.sourceStream == null;
        }

        @Override
        public boolean isReady() {
            return this.sourceStream != null;
        }

        @Override
        public void setReadListener(ReadListener listener) {
            // TODO document why this method is empty
        }
    }

    @Test
    void getPVNamesFromPostBody() throws IOException {
        String testPVsJson = "[\"test1\",\"test2\"]";
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getContentType()).thenReturn("application/json");
        when(request.getMethod()).thenReturn("POST");
        DelegatingServletInputStream inputStream =
                new DelegatingServletInputStream(new ByteArrayInputStream(testPVsJson.getBytes()));
        when(request.getInputStream()).thenReturn(inputStream);
        try {
            List<String> pvs = PVsMatchingParameter.getPVNamesFromPostBody(request);
            assertEquals(2, pvs.size());
            assertTrue(pvs.contains("test1"));
            assertTrue(pvs.contains("test2"));
        } catch (Exception e) {
            fail(e);
        }
    }
}
