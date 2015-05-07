<%@ page contentType="text/html;charset=UTF-8" pageEncoding="UTF-8" %>
<%@page 
	import="org.apache.hadoop.fs.Path"
	import="org.apache.hadoop.fs.FileStatus"
	import="java.net.*"
	import="java.util.HashMap"
	import="java.io.File"
	import="org.apache.hadoop.fs.hdfsoverhttp.*"
%>
<jsp:useBean id="dirList" scope="request" class="java.util.ArrayList"/>
<jsp:useBean id="fileList" scope="request" class="java.util.ArrayList"/>
<%
	String targetDir = (String)request.getAttribute("target");
	String contextAndServletPath = (String)request.getAttribute("contextandservletpath");
	DirectoryMetaInfo dirMetaInfo = (DirectoryMetaInfo)request.getAttribute("dirMetaInfo");
	int nameColMaxLength = dirMetaInfo.getFileNameMaxLength() + 2;
	int sizeColMaxLength = dirMetaInfo.getFileSizeMaxLength() + 2;
	String orderType = (String)request.getAttribute("orderType");
%>
<html>
 <head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Index of <%=targetDir%></title>
 </head>
<body>
<%
	HdfsTool hdfsTool = null;
	FileStatus targetFileStatus = null;
	
	try{
		hdfsTool = new HdfsTool(getServletContext());
	}catch(HdfsInitException e){
		response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "can't connect to hdfs");
		return;
	}

	if(SysConfig.hasHeader && dirMetaInfo.isHeaderExist()){		
		if(hdfsTool.includeFile(targetDir,SysConfig.HEADER_FILE, out) == false){
	       	response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "read " + SysConfig.HEADER_FILE + " error");
		}
	}
%>
<h1>
<%
	if(SysConfig.hasHeader == false ||  dirMetaInfo.isHeaderExist() == false){	
		if (fileList == null) {
		    out.print("directory : " + targetDir + " does not exist");
		}else{
		%>
		Index of directory
		<%
		    out.print("/");
			String[] parts = targetDir.split(Path.SEPARATOR);
			if(parts.length > 0) {
				out.print(parts[parts.length-1]);
			}
		}
	}
%>
</h1>
<pre>
<img src="<%=contextAndServletPath%>/.icons/blank.gif" alt="[Icon]"><a href="?C=N&O=<%=orderType%>">Name</a><%=DirectoryMetaInfo.createBlankTag(nameColMaxLength - 4) %><a href="?C=M&O=<%=orderType%>">Last modified</a><%=DirectoryMetaInfo.createBlankTag(6) %><a href="?C=S&O=<%=orderType%>">Size</a><%=DirectoryMetaInfo.createBlankTag(sizeColMaxLength) %><a href="?C=D&O=<%=orderType%>">Description</a>
<hr><%if(request.getAttribute("parentdir") != ""){
	out.print("<img src=\"" + contextAndServletPath + "/.icons/back.gif\" alt=\"[DIR ]\">&nbsp;<a href=\"../\">Parent Directory</a>");
	out.print(DirectoryMetaInfo.createBlankTag(nameColMaxLength + 6) + "-");
	out.print("<br>");
}%><%
	for (int i=0; i < dirList.size(); i++) {
		String[] cols = (String[])dirList.get(i);
		String targetUrl = null;
		targetUrl = "<a href=\""
				+ contextAndServletPath
				+ targetDir
				+ URLEncoder.encode(cols[0],"UTF-8")
				+ "/\">"
				+ cols[0]
				+ "/</a>"
				+ DirectoryMetaInfo.createBlankTag(nameColMaxLength,cols[0].getBytes().length + 1,cols[0].length() + 1);
		out.print("<img width=\"20px\" src=\"" + contextAndServletPath + "/.icons/folder.gif\" alt=\"[DIR ]\">&nbsp;" + targetUrl + cols[5] + "    -   ");           
		out.print("<br>");
	}
	for (int i=0; i < fileList.size(); i++) {
		String[] cols = (String[])fileList.get(i);
		String targetUrl = null;
		String fileExt = cols[0].substring(cols[0].lastIndexOf(".") + 1);
		String iconFileName = IconRegister.getIcon(
				getServletContext().getRealPath("/") + ".icons",
				fileExt
			);
		targetUrl = "<a href=\"" + URLEncoder.encode(cols[0],"UTF-8")
		+ "\">"
		+ cols[0]
		+ "</a>"
		+ DirectoryMetaInfo.createBlankTag(nameColMaxLength,cols[0].getBytes().length,cols[0].length());
           out.print("<img width=\"20px\" src=\"" + contextAndServletPath + "/.icons/" + iconFileName + "\"  alt=\"[FILE]\">&nbsp;" + targetUrl +  cols[5] + "  " + cols[2]);
   		out.print("<br>");
	}
%><hr>
</pre>
<%
if(SysConfig.hasReadme && dirMetaInfo.isReadmeExist()){
	if(hdfsTool.includeFile(targetDir,SysConfig.README_FILE, out) == false){
		response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "read " + SysConfig.README_FILE + " error");
	}
}
%>
</body>
</html>
