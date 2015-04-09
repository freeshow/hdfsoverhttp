/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.fs.hdfsoverhttp;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class FileDownloadServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
    /**
     * The output buffer size to use when serving resources.
     */
    protected int output = 2048;
    
	/**
     * MIME multipart separation string
     */
    protected static final String mimeSeparation = "CATALINA_MIME_BOUNDARY";
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public FileDownloadServlet() {
        super();
    }    
    
    /**
     * Process a HEAD request for the specified file or directory on hdfs.
     *
     * @param request The servlet request we are processing
     * @param response The servlet response we are creating
     *
     * @exception IOException if an input/output error occurs
     * @exception ServletException if a servlet-specified error occurs
     */    
    protected void doHead(HttpServletRequest request, HttpServletResponse response)
    	throws ServletException, IOException {
        // Serve the requested resource, without the data content
        serveResource(request, response, false);

    }
    
    /**
     * Process a GET request for the specified file or directory on hdfs.
     *
     * @param request The servlet request we are processing
     * @param response The servlet response we are creating
     *
     * @exception IOException if an input/output error occurs
     * @exception ServletException if a servlet-specified error occurs
     */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
		throws ServletException, IOException {

		// Serve the requested resource, including the data content
        serveResource(request, response, true);		

	}

    /**
     * Process a POST request for the specified file or directory on hdfs.
     *
     * @param request The servlet request we are processing
     * @param response The servlet response we are creating
     *
     * @exception IOException if an input/output error occurs
     * @exception ServletException if a servlet-specified error occurs
     */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
	
    /**
     * Serve the specified file or directory on hdfs, optionally including the data content.
     *
     * @param request The servlet request we are processing
     * @param response The servlet response we are creating
     * @param content Should the content be included?
     *
     * @exception IOException if an input/output error occurs
     * @exception ServletException if a servlet-specified error occurs
     */
    protected void serveResource(HttpServletRequest request,
                                 HttpServletResponse response,
                                 boolean content)
        throws IOException, ServletException {	
		String uri = request.getRequestURI();
		String contextPath = (String)request.getContextPath();
		String servletPath = request.getServletPath();
		String orginalTarget = uri.substring(contextPath.length() + servletPath.length());
		String decodeTarget = URLDecoder.decode(orginalTarget,"UTF-8");
		if(decodeTarget.equals(""))decodeTarget = "/";
		request.setAttribute("contextandservletpath", contextPath + servletPath);
		if(decodeTarget.endsWith("/")){  // if it is directory
			String orderCol = request.getParameter("C");
			String orderType = request.getParameter("O");
			if(orderType == null)orderType = "";
			showDirList(request,response,decodeTarget,orderCol,orderType);
		}else{  // if it is file
			fileDownload(request,response,decodeTarget,content);
		}
    	
    }
	
	/**
	 * show file list of a directory 
	 * @param request
	 * @param response
	 * @param targetDir
	 * @throws ServletException
	 * @throws IOException
	 */
	protected void showDirList(HttpServletRequest request, HttpServletResponse response,String targetDir,String orderCol,String orderType)
		throws ServletException, IOException{
		
		HdfsTool hdfsTool = null;
		try{
			hdfsTool = new HdfsTool(getServletContext());
		}catch(HdfsInitException e){
	    	response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "can't connect to hdfs");
	    	return;
		}
		
	    if (targetDir == null || targetDir.length() == 0) {
	    	targetDir = Path.SEPARATOR;
	    }
	    
	    // if index existed then show it
	    try{
	    	if(showIndex(request,response,targetDir,hdfsTool))return;
	    }catch(Exception e){
    		response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
	    	return;
	    }

	    request.setAttribute("target", targetDir);
	    ArrayList<String[]> fileList = new ArrayList<String[]>();
	    ArrayList<String[]> dirList = new ArrayList<String[]>();
	    DirectoryMetaInfo dirMetaInfo = new DirectoryMetaInfo();
	    int errCode = hdfsTool.listFiles(targetDir,dirList,fileList,dirMetaInfo);
	    if(errCode < 0){
		    switch(errCode){
		    	case -1:
		    		request.setAttribute("originalurl", request.getRequestURI());
					response.sendError(HttpServletResponse.SC_NOT_FOUND, targetDir + " doesn't exist");
					break;
		    	case -9:
		    		response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "hdfs status is corrupt");
		        	break;
		    }
		    return;
	    }else{
        	if(orderCol == null){
        		request.setAttribute("dirList",dirList );
        		request.setAttribute("fileList",fileList );
        	}else{
	        	ArrayList<String[]> sortedDirList = new ArrayList<String[]>();
	        	hdfsTool.sortFileList(dirList, orderCol, orderType, sortedDirList);
	        	ArrayList<String[]> sortedFileList = new ArrayList<String[]>();
	        	hdfsTool.sortFileList(fileList, orderCol, orderType, sortedFileList);
			    request.setAttribute("dirList",sortedDirList );
			    request.setAttribute("fileList",sortedFileList );
        	}
		    request.setAttribute("parentdir", hdfsTool.getParentDir());
		    request.setAttribute("dirMetaInfo", dirMetaInfo);
		    request.setAttribute("orderType",orderType.equals("D")?"A":"D");
		    
		    // redirect to file list page
		    ServletContext sc = getServletContext();
		    RequestDispatcher rd = sc.getRequestDispatcher("/fileList.jsp");
		    rd.forward(request,response);
		    return;
	    }
	}
	
	/**
	 * down a file
	 * @param request
	 * @param response
	 * @param targetFile
	 * @throws ServletException
	 * @throws IOException
	 */
	protected void fileDownload(HttpServletRequest request, HttpServletResponse response,String targetFile,boolean content)
		throws ServletException, IOException{
		
		String targetFileName = targetFile.substring(targetFile.lastIndexOf("/") + 1);
		//String orginalFileName = new String(targetFileName.getBytes("UTF-8"), "ISO-8859-1");
		String targetDir = targetFile.substring(0,targetFile.lastIndexOf("/"));
		
		HdfsTool hdfsTool = null;
		try{
			hdfsTool = new HdfsTool(getServletContext());
		}catch(HdfsInitException e){
	    	response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	    	return;
		}

		FileStatus targetFileStatus = null;
		Object result = hdfsTool.checkFile(targetDir,targetFileName);
		if(result.getClass().equals(FileStatus.class)){
			targetFileStatus = (FileStatus)result;
		}else{
			int errCode = (Integer)result;
			if(errCode < 0){
				switch(errCode){
			    	case -1:
				    	request.setAttribute("originalurl", request.getRequestURI());
						response.sendError(HttpServletResponse.SC_NOT_FOUND);
						break;
			    	case -2:
						response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			        	break;
				}
		    	return;
			}			
		}

        String contentType = getServletContext().getMimeType(targetFileName);
        if(contentType == null)contentType = "application/octet-stream";
		response.setContentType(contentType);
		response.setCharacterEncoding("UTF-8");

		// Accept ranges header
        response.setHeader("Accept-Ranges", "bytes");
        
		Range range = new Range();
        // Parse range specifier
        long contentLength = -1L;
        
		ArrayList<Range> ranges = range.parseRange(request, response, targetFileStatus);        
        // ETag header
        response.setHeader("ETag", Range.getETag(targetFileStatus));
        // Last-Modified header
        response.setHeader("Last-Modified",Range.getLastModifiedHttp(targetFileStatus));
        // Get content length
        if(targetFileStatus != null)
        	contentLength = targetFileStatus.getLen();
        
        response.setBufferSize(output);
        
        // retrieve the servlet output stream
        ServletOutputStream os = response.getOutputStream();
        
		if (  (((ranges == null) || (ranges.isEmpty()))
                        && (request.getHeader("Range") == null) )
                || (ranges == Range.FULL) ) {
			
            // Set the content-length as String to be able to use a long
            if (contentLength < Integer.MAX_VALUE) {
                response.setContentLength((int) contentLength);
            } else {
                // Set the content-length as String to be able to use a long
                response.setHeader("content-length", "" + contentLength);
            }
            if(content){
            	if(hdfsTool.copyFull(targetFileStatus, os) == false){
            		response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		return;
            	}
            }
		}else{
			response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
            if (ranges.size() == 1) {
	            Range range0 = ranges.get(0);
	            response.addHeader("Content-Range", "bytes "
	                               + range0.start
	                               + "-" + range0.end + "/"
	                               + range0.length);
	            long length = range0.end - range0.start + 1;
                if (length < Integer.MAX_VALUE) {
                    response.setContentLength((int) length);
                } else {
                    // Set the content-length as String to be able to use a long
                    response.setHeader("content-length", "" + length);
                }
                
	            if(content){
	            	if(hdfsTool.copyRange(targetFileStatus, os, range0) == false){
	            		response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
	            		return;
	            	}
	            }
            }else{
                response.setContentType("multipart/byteranges; boundary="
                        + mimeSeparation);
				if (content) {
					for(int i = 0;i < ranges.size();i ++){
			            Range currentRange = ranges.get(i);

			            // Writing MIME header.
			            os.println();
			            os.println("--" + mimeSeparation);
			            if (contentType != null)
			                os.println("Content-Type: " + contentType);
			            os.println("Content-Range: bytes " + currentRange.start
			                           + "-" + currentRange.end + "/"
			                           + currentRange.length);
			            os.println();

			            // Printing content
			            if(hdfsTool.copyRange(targetFileStatus, os, currentRange) == false){
			            	response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			            	break;
			            }
			        }
			        os.println();
			        os.print("--" + mimeSeparation + "--");					
				}
            }
		}
	}
	
	/**
	 * if index.html exist and show it
	 * @param request
	 * @param response
	 * @param targetDir
	 * @param hdfsTool
	 * @return show result(true or false)
	 * @throws Exception
	 */
	protected boolean showIndex(HttpServletRequest request, HttpServletResponse response,String targetDir,HdfsTool hdfsTool)
		throws Exception{
		FileStatus targetFileStatus = null;
		Object result = hdfsTool.checkFile(targetDir,SysConfig.INDEX_HTML);
		if(result.getClass().equals(FileStatus.class)){
			targetFileStatus = (FileStatus)result;
			response.setContentType("text/html");
			response.setCharacterEncoding("UTF-8");
			// Accept ranges header
	        response.setHeader("Accept-Ranges", "");
        	long contentLength = targetFileStatus.getLen();
            if (contentLength < Integer.MAX_VALUE) {
                response.setContentLength((int) contentLength);
            } else {
                // Set the content-length as String to be able to use a long
                response.setHeader("content-length", "" + contentLength);
            }
	        // retrieve the servlet output stream
	        ServletOutputStream os = response.getOutputStream();
           	if(hdfsTool.copyFull(targetFileStatus, os) == false){
           		throw new Exception("read " + SysConfig.INDEX_HTML + " error");
           	}else{
           		return true;
           	}
		}else
			return false;
	}
	
}
