## Information useful to developers

1. If you unfamiliar with servlet containers; here\'s a small overview
   that a few collaborators found useful
   - Reading up on a few basics will help; there are several good
     sources of information on the net; but don\'t get bogged down by
     the details.
   - Please do use Eclipse/Netbeans/Intelij to navigate the code.
     This makes life so much easier.
   - To get a quick sense of what a class/interface does, you can use
     the [javadoc](../_static/javadoc/index.html). Some attempts have been made to
     have some Javadoc in most classes and all interfaces
   - We use Tomcat purely as a servlet container; that is, a quick
     way of servicing HTTP requests using Java code.
   - A WAR file is basically a ZIP file (you can use unzip) with some
     conventions. For example, all the libraries (.jar\'s) that the
     application depends on will be located in the WEB-INF/lib
     folder.
   - The starting point for servlet\'s in a WAR file is the file
     `WEB-INF/web.xml`. For example, in the mgmt.war\'s
     `WEB-INF/web.xml`, you can see that all HTTP requests matching
     the pattern `/bpl/*` are satisfied using the Java class
     `org.epics.archiverappliance.mgmt.BPLServlet`.
   - If you navigate to this class in Eclipse, you\'ll see that it is
     basically a registry of BPLActions.
   - For example, the HTTP request, `/mgmt/bpl/getAllPVs` is
     satisfied using the `GetAllPVs` class. Breaking this down,
     1. `/mgmt` gets you into the mgmt.war file.
     2. `/bpl` gets you into the BPLServlet class.
     3. `/getAllPVs` gets you into the GetAllPVs class.
   - From a very high level
     - The engine.war establishes Channel Access monitors and then
       writes the data into the short term store (STS).
     - The etl.war file moves data between stores - that is from
       the STS to the MTS and from the MTS to the LTS and so on.
     - The retrieval.war gathers data from all the stores, stitches
       them together to satisfy data retrieval requests.
     - The mgmt.war manages all the other three and holds
       configuration state.
   - In terms of configuration, the most important is the
     `PVTypeInfo`; you can see what one looks like by looking at
     <http://machine:17665/mgmt/bpl/getPVTypeInfo?pv=MYPV:111:BDES>
   - The main interfaces are the ones in the
     [`org.epics.archiverappliance`](../_static/javadoc/org/epics/archiverappliance/package-summary.html)
     package.
   - The
     [ConfigService](../_static/javadoc/org/epics/archiverappliance/config/ConfigService.html)
     class does all configuration management.
   - The [customization guide](../../sysadmin/guides/customization) is also a good
     guide to way in which this product can be customized.

## ConfigService

All of the configuration in the archiver appliance is handled thru
implementations of the
[ConfigService](../_static/javadoc/org/epics/archiverappliance/config/ConfigService.html)
interface. Each webapp has one instance of this interface and this
instance is dependency injected into the classes that need it. If all
else fails, you can create your implementation of the ConfigService and
register it in the servlet context
[listener](../_static/javadoc/org/epics/archiverappliance/config/ArchServletContextListener.html).
