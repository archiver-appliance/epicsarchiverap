package org.epics.archiverappliance.common.taglets;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

/**
 * Called as part of the javadoc task;
 * This loads the javadoc generated file docs/api/mgmt_scriptables.txt and uses it to generate the docs/api/mgmt_scriptables.html document.
 * The docs/api/mgmt_scriptables.html contains a list of all the BPL's that can be accessed from outside the system (perhaps thru python/bash)
 * @author mshankar
 *
 */
public class ProcessMgmtScriptables {

    /**
     * @param args  &emsp;
     * @throws Exception  &emsp;
     */
    public static void main(String[] args) throws Exception {
        class BPLParam {
            String paramName;
            String paramDesc;

            public BPLParam(LinkedList<String> lines) {
                this.paramName = lines.get(0);
                this.paramDesc = lines.get(1);
            }
        }

        class BPLActionDetail {
            String path;
            String bplclass;
            String actiondesc;
            LinkedList<BPLParam> paramDesc = new LinkedList<BPLParam>();

            public BPLActionDetail(LinkedList<String> lines) {
                this.path = lines.get(0);
                this.bplclass = lines.get(1);
                this.actiondesc = lines.get(2);
            }
        }

        //		@StartMethod
        //		/getPVStatus
        //		org.epics.archiverappliance.mgmt.bpl.GetPVStatusAction
        //		- Get the status of a PV.
        //		@MethodDescDone
        //		@StartParam
        //		pv
        //		 The name of the pv for which status is to be determined.
        //		@EndParam
        //		@EndMethod

        LinkedList<BPLActionDetail> actionDetails = new LinkedList<BPLActionDetail>();
        BPLActionDetail currentAction = null;
        LinkedList<String> lines = null;
        try (LineNumberReader in = new LineNumberReader(
                new InputStreamReader(new FileInputStream(new File("docs/api/mgmt_scriptables.txt"))))) {
            String line = in.readLine();
            while (line != null) {
                switch (line) {
                    case "@StartMethod": {
                        currentAction = null;
                        lines = new LinkedList<String>();
                        break;
                    }
                    case "@MethodDescDone": {
                        currentAction = new BPLActionDetail(lines);
                        lines = new LinkedList<String>();
                        break;
                    }
                    case "@StartParam": {
                        lines = new LinkedList<String>();
                        break;
                    }
                    case "@EndParam": {
                        currentAction.paramDesc.add(new BPLParam(lines));
                        lines = new LinkedList<String>();
                        break;
                    }
                    case "@EndMethod": {
                        actionDetails.add(currentAction);
                        currentAction = null;
                        lines = new LinkedList<String>();
                        break;
                    }
                    default: {
                        lines.add(line);
                        break;
                    }
                }
                line = in.readLine();
            }
        }

        // We want to sort actionDetails according to the location in the BPLServlet.
        // This is output to mgmtpathmappings.txt as part of the javadoc ant task.
        // We read the sequence from there. Here's a sample

        //    	#Path mappings for mgmt BPLs
        //    	#Tue Oct 16 18:10:26 PDT 2012
        //    	/resumeArchivingPV=org.epics.archiverappliance.mgmt.bpl.ResumeArchivingPV
        final LinkedList<String> pathsInBPLServletSequence = new LinkedList<String>();
        try (LineNumberReader in = new LineNumberReader(
                new InputStreamReader(new FileInputStream(new File("docs/api/mgmtpathmappings.txt"))))) {
            String line = in.readLine();
            while (line != null) {
                if (!line.startsWith("#") && line.contains("=")) {
                    String[] parts = line.split("=");
                    String path = parts[0];
                    pathsInBPLServletSequence.add(path);
                }
                line = in.readLine();
            }
        }

        // Now do the sort.
        Collections.sort(actionDetails, new Comparator<BPLActionDetail>() {
            @Override
            public int compare(BPLActionDetail o1, BPLActionDetail o2) {
                int posn1 = pathsInBPLServletSequence.indexOf(o1.path);
                int posn2 = pathsInBPLServletSequence.indexOf(o2.path);
                return posn1 - posn2;
            }
        });

        // We get the template and replace the @Content tag with the generated content.
        try (LineNumberReader in = new LineNumberReader(new InputStreamReader(
                        new FileInputStream(new File("docs/templates/mgmt_scriptables_template.html"))));
                PrintWriter out =
                        new PrintWriter(new FileOutputStream(new File("docs/api/mgmt_scriptables.html"), false))) {
            // Copy the template till we come to @Content
            String line = in.readLine();
            while (line != null) {
                if (line.startsWith("@Content")) {
                    break;
                }
                out.println(line);
                line = in.readLine();
            }

            for (BPLActionDetail actionDetail : actionDetails) {
                out.println("<div class=\"bplelement\"><h1>");
                out.println(actionDetail.path);
                out.println("</h1><div>");
                out.println(actionDetail.actiondesc);
                out.print("</div><br/>For more details, please see the <a href=\"");
                out.print(actionDetail.bplclass.replace('.', '/'));
                out.println(".html\">javadoc</a><div><dl>");
                for (BPLParam param : actionDetail.paramDesc) {
                    out.println("<dt>");
                    out.println(param.paramName);
                    out.println("</dt>");
                    out.println("<dd>");
                    out.println(param.paramDesc);
                    out.println("</dd>");
                }
                out.println("</dl></div>");
                out.println("</div>");
            }

            // Copy the rest of the template
            line = in.readLine();
            while (line != null) {
                out.println(line);
                line = in.readLine();
            }
        }
    }
}
