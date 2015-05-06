package org.epics.archiverappliance.mgmt.archivepv;

import org.epics.archiverappliance.config.ApplianceInfo;

public class ApplianceAndTotalRate {

        private ApplianceInfo  appInfo;
        private float totalDataRate;
        public ApplianceAndTotalRate(ApplianceInfo  appInfo,float totalDataRate)
        {
                this.appInfo=appInfo;
                this.totalDataRate=totalDataRate;
        }
        public float getTotalDataRate() {
                return totalDataRate;
        }
        public void setTotalDataRate(float totalDataRate) {
                this.totalDataRate = totalDataRate;
        }
        public ApplianceInfo getAppInfo() {
                return appInfo;
        }
}
