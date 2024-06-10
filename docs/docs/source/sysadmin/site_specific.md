# Making simple site-specific content changes

While the [Customization Guide](customization) has details on a
more flexible mechanism to customize the EPICS archiver appliance on a
per-site basis, often what is required is a simple replacement of the
images and text that are on the various pages. This can be done after
unpacking the WAR file during deployment. The `quickstart.sh` and
`single_machine_install.sh` scripts has some sample code that does this
that can be used as a starting point.

- These rely on the presence of a folder `site_specific_content` in
    the deployment folder.
- All images in the folder `site_specific_content/img` is copied into
    the mgmt webapp\'s `ui/comm/img` folder and can be used for site
    logos and so on.
- The [SyncStaticContentHeadersFooters](api/org/epics/archiverappliance/mgmt/bpl/SyncStaticContentHeadersFooters.html)
    is run on a file called `template_changes.html`

At a high level, after unzipping the `mgmt.war` into the Tomcat
`webapps` folder, one can replace/modify the files to implement some
simple content changes. The various HTML files that comprise the UI have
sections delimited by special tags. For example, the home page has

```html
<!-- @begin(site_contact_text) -->
This is the archiver appliance management console for the ... 
<!-- @end(site_contact_text) -->
```

The simple java utility
[SyncStaticContentHeadersFooters](api/org/epics/archiverappliance/mgmt/bpl/SyncStaticContentHeadersFooters.html)
can be used to replace content between these tags based on a master
template. For example, create a master temple in
`/tmp/master_template.html` that looks like so

```html
    <!-- @begin(site_header) -->
    <div class="pageheader" style="background-image:url('comm/img/mylab.png'); background-size:1024px 400px;">
    <span class="apptitle" id="archiveInstallationTitle">My Site Archiver Appliance</span>
    </div>
    <!-- @end(site_header) -->

    <!-- @begin(site_contact_text) -->
    This is the EPICS archiver appliance management console for my site/beamline/program. Please contact me at 555 - 555 - 5555 if you have any issues. 
    <!-- @end(site_contact_text) -->
```

Then run `SyncStaticContentHeadersFooters` using
`/tmp/master_template.html` as the master template like so

```bash
java -cp ${TOMCAT_HOME}/webapps/mgmt/WEB-INF/classes:${TOMCAT_HOME}/webapps/mgmt/WEB-INF/lib/log4j-1.2.17.jar \
    org.epics.archiverappliance.mgmt.bpl.SyncStaticContentHeadersFooters \
    /tmp/master_template.html \
    ${TOMCAT_HOME}/webapps/mgmt/ui
```

to generate

![image](../images/simple_static_content.png)
