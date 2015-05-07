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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Locale;
import java.util.TimeZone;
import java.util.Vector;

import javax.servlet.ServletContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class HdfsTool {
	private static final DecimalFormat decimalFormat;
	private static final DecimalFormat sizeFormat;
	private static final SimpleDateFormat dateForm = new SimpleDateFormat(
			"dd-MMM-yyyy HH:mm", Locale.US);
	protected final static TimeZone gmtZone = TimeZone.getTimeZone("GMT");
	static Logger log = Logger.getLogger(HdfsTool.class);

	static {
		NumberFormat numberFormat = NumberFormat.getNumberInstance(Locale.US);
		decimalFormat = (DecimalFormat) numberFormat;
		decimalFormat.applyPattern("#.#");
		sizeFormat = (DecimalFormat) NumberFormat.getNumberInstance(Locale.US);
		sizeFormat.applyPattern("000000000000");
		dateForm.setTimeZone(gmtZone);
	}

	private static FileSystem dfs = null;

	static Path ROOT_DIR_PATH;
	private String targetDir;
	private static final int BUFFER_SIZE = 2048;
	private UserGroupInformation ugi;
	private String userName = "guest";
	private String[] groupNames = new String[] { "guest" };

	/**
	 * 
	 * @param context
	 * @throws HdfsException
	 * @throws IOException
	 */
	public HdfsTool(ServletContext context) throws HdfsException {
		if (dfs == null) {
			SysConfig.init(context);
			dfs = getDfs(SysConfig.HDFS_URI);
			if (dfs == null) {
				throw new HdfsException("can't connect to hdfs");
			}
			
			try {
				ugi = UserGroupInformation.getLoginUser();
				userName = ugi.getUserName();
				groupNames = ugi.getGroupNames();
			} catch (IOException e) {
				log.error("exception when get os user and group", e);
			}
		}
	}

	/**
	 * HDFS Initialization
	 * 
	 * @param hdfsUri
	 * @return handle of file system
	 * @throws IOException
	 */
	private FileSystem getDfs(String hdfsUri) {
		FileSystem dfs = new DistributedFileSystem();
		Configuration conf = new Configuration();
		try {
			dfs.initialize(new URI(hdfsUri), conf);
			return dfs;
		} catch (URISyntaxException e) {
			log.error("hdfsUri is invalid", e);
		} catch (IOException e) {
			log.error("DFS Initialization error", e);
		} catch (Exception e) {
			log.error("unknown exception", e);
		}
		return null;
	}

	/**
	 * get directory and file list
	 * 
	 * @param targetDir
	 * @param fileList
	 * @param metaInfo
	 * @return error code
	 * @throws IOException
	 */
	public int listFiles(String targetDir, ArrayList<String[]> dirList,
			ArrayList<String[]> fileList, DirectoryMetaInfo metaInfo)
			throws IOException {
		if (targetDir.startsWith(Path.SEPARATOR) == false) {
			targetDir = Path.SEPARATOR + targetDir;
		}
		targetDir = convertInvalidChar(targetDir);
		this.targetDir = targetDir;
		
		Path dstPath = new Path(SysConfig.ROOT_DIR + targetDir);
		FileStatus targetDirStatus = null;
		// exist check
		try {
			targetDirStatus = dfs.getFileStatus(dstPath);
			log.debug("list files for " + dstPath);
		} catch (ConnectException ce) {
			log.error("ConnectException", ce);
			return -9;
		} catch (FileNotFoundException fe) {
			log.error(dstPath + " doesn't exist");
			return -1;
		}

		// check it is directory
		if (!targetDirStatus.isDirectory()) {
			log.error(dstPath + " is not a directory");
			return -1;
		}
		// check permission
		if (!hasExecutePermission(targetDirStatus)) {
			log.error(dstPath + ": Permission denied");
			return -1;
		}
		FileStatus[] files = dfs.listStatus(dstPath);
		for (int i = 0; i < files.length; i++) {

			if (files[i].getPath().getName().equals(SysConfig.HEADER_FILE)) {
				metaInfo.setHeaderExist(true);
				continue;
			}
			if (files[i].getPath().getName().equals(SysConfig.README_FILE)) {
				metaInfo.setReadmeExist(true);
			}

			if (files[i].isDirectory()) {
				if (!hasExecutePermission(files[i]))
					continue;
			} else {
				if (!hasReadPermission(files[i]))
					continue;
			}

			String cols[] = new String[10];
			cols[0] = revertInvalidChar(files[i].getPath().getName());
			metaInfo.setFileNameMaxLength(cols[0].getBytes().length,
					cols[0].length());
			cols[5] = dateForm
					.format(new Date((files[i].getModificationTime())));
			if (!files[i].isDirectory()) {
				cols[1] = "file";
				cols[2] = byteDesc(files[i].getLen());
				metaInfo.setFileSizeMaxLength(cols[2].length());
				cols[9] = sizeFormat.format(files[i].getLen());
				fileList.add(cols);
			} else {
				cols[1] = "dir";
				cols[2] = "";
				cols[3] = "";
				cols[4] = "";
				cols[9] = "";
				dirList.add(cols);
			}
		}
		return 1;
	}

	/**
	 * sort the file list
	 * 
	 * @param fileList
	 * @param col
	 *            : the name of column,N:Name,M:modificationTime,S:size
	 * @param orderType
	 *            : D:desc,A:asc
	 */
	public void sortFileList(ArrayList<String[]> fileList, String col,
			String orderType, ArrayList<String[]> sortedFileList) {
		Hashtable<String, String[]> h = new Hashtable<String, String[]>();
		for (int i = 0; i < fileList.size(); i++) {
			String[] cols = (String[]) fileList.get(i);
			if (col.equals("N"))
				h.put(cols[0] + "-" + i, cols);
			else if (col.equals("M")) {
				h.put(cols[5] + "-" + i, cols);
			} else if (col.equals("S")) {
				h.put(cols[9] + "-" + i, cols);
			} else {
				h.put(cols[0] + "-" + i, cols);
			}
		}
		Vector<String> v = new Vector<String>(h.keySet());
		Collections.sort(v);
		if (orderType.equals("D")) {
			for (int i = v.size(); i > 0; i--) {
				sortedFileList.add((String[]) h.get(v.get(i - 1)));
			}
		} else {
			Iterator<String> it = v.iterator();
			while (it.hasNext()) {
				String element = (String) it.next();
				sortedFileList.add((String[]) h.get(element));
			}
		}
	}

	/**
	 * 
	 * @return current target directory
	 */
	String getTargetDir() {
		return this.targetDir;
	}

	/**
	 * get parent directory
	 * 
	 * @return parent directory
	 */
	String getParentDir() {
		Path p = new Path(SysConfig.ROOT_DIR + this.targetDir);
		if (p.getParent() == null) {
			return "";
		}
		String parent = p.getParent().toUri().toString();
		if (parent != null && parent.length() >= SysConfig.ROOT_DIR.length()) {
			parent = parent.substring(SysConfig.ROOT_DIR.length());
			if (parent.length() == 0)
				parent = Path.SEPARATOR;
		} else {
			parent = "";
		}
		return parent;
	}

	/**
	 * check file validity
	 * 
	 * @param targetDir
	 * @param targetFileName
	 * @param existCheckOnly
	 * @return error code or file status object
	 */
	public Object checkFile(String targetDir, String targetFileName) {
		Path targetFile = null;
		if((targetDir).equals(Path.SEPARATOR)) {
			targetFile = new Path(convertInvalidChar(SysConfig.ROOT_DIR + targetDir + targetFileName));
		}else{
			targetFile = new Path(convertInvalidChar(SysConfig.ROOT_DIR + targetDir + Path.SEPARATOR + targetFileName));
		}

		try{
			if(!dfs.exists(targetFile)){
				return new Integer(-1);
			}
		} catch (IOException ce) {
			log.error("IOException", ce);
			return new Integer(-2);
		}
		
		// exist check
		FileStatus targetFileStatus = null;
		try {
			targetFileStatus = dfs.getFileStatus(targetFile);
		} catch (Exception e){
			log.error("unkown exception:", e);
			return new Integer(-3);
		}

		// file or directory check
		if (targetFileStatus.isDirectory()) {
			log.warn(targetFile.toUri().toString() + " is a directory");
			return new Integer(-4);
		}

		// permission check
		if (!hasReadPermission(targetFileStatus)) {
			log.warn("PERMISSIONS: " + targetFile.toString() + " - "
					+ " read denied");
			return new Integer(-5);
		}
		return targetFileStatus;
	}

	/**
	 * copy all contents of the specified file to client
	 * 
	 * @param targetFileStatus
	 * @param ostream
	 * @return copy result(true or false)
	 */
	public boolean copyFull(FileStatus targetFileStatus,
			ServletOutputStream ostream) {
		FSDataInputStream is = null;
		int bytesToRead = 0;
		byte[] buffer = new byte[BUFFER_SIZE];
		try {
			is = dfs.open(targetFileStatus.getPath());
			while ((bytesToRead = is.read(buffer)) != -1) {
				try {
					ostream.write(buffer, 0, bytesToRead);
				} catch (IOException e) {
					log.warn("write file "
							+ targetFileStatus.getPath()
							+ " be aborted \n"
							+ "ClientAbortException:  java.net.SocketException: ",
							e);
					return true;
				}
			}
		} catch (IOException e) {
			log.error("read file " + targetFileStatus.getPath() + " error ", e);
			return false;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return true;
	}

	/**
	 * include file contents for other file
	 * 
	 * @param targetDir
	 * @param targetFileName
	 * @param out
	 * @return copy result(true or false)
	 */
	public boolean includeFile(String targetDir, String targetFileName,
			JspWriter out) {

		Path targetFile = new Path(convertInvalidChar(SysConfig.ROOT_DIR
				+ targetDir + Path.SEPARATOR + targetFileName));
		FileStatus targetFileStatus = null;
		FSDataInputStream is = null;

		try {
			targetFileStatus = dfs.getFileStatus(targetFile);
			is = dfs.open(targetFileStatus.getPath());
			byte[] buffer = new byte[is.available()];
			is.readFully(buffer);
			out.write(new String(buffer));
		} catch (IOException e) {
			log.error("read file " + targetFileStatus.getPath() + " error ", e);
			return false;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return true;
	}

	/**
	 * Copy the contents of the specified file to the specified output stream,
	 * and ensure that both streams are closed before returning (even in the
	 * face of an exception).
	 * 
	 * @param targetFileStatus
	 *            The file status object
	 * @param ostream
	 *            The output stream to write to
	 * @param range
	 *            Start and of the range which will be copied
	 * @return Exception which occurred during processing
	 * @throws IOException
	 */
	public boolean copyRange(FileStatus targetFileStatus,
			ServletOutputStream ostream, Range range) {
		FSDataInputStream is = null;
		int bytesToRead = 0;
		long shouldReadLength = range.end - range.start + 1;
		long readedLength = 0;
		byte[] buffer = new byte[BUFFER_SIZE];
		try {
			is = dfs.open(targetFileStatus.getPath());
			is.seek(range.start);
			while ((bytesToRead = is.read(buffer)) != -1) {
				readedLength += bytesToRead;
				try {
					if (readedLength >= shouldReadLength) {
						ostream.write(buffer, 0, bytesToRead
								- (int) (readedLength - shouldReadLength));
						break;
					} else {
						ostream.write(buffer, 0, bytesToRead);
					}
				} catch (IOException e) {
					log.info("write file "
							+ targetFileStatus.getPath()
							+ " be aborted \n"
							+ "ClientAbortException:  java.net.SocketException: ",
							e);
					return true;
				}

			}
		} catch (IOException e) {
			log.error("read file " + targetFileStatus.getPath() + " error ", e);
			return false;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return true;
	}

	/**
	 * convert [:] into [%3a] for file name
	 * 
	 * @param fileName
	 * @return converted filename
	 */
	String convertInvalidChar(String fileName) {
		if (fileName.indexOf(":") > 0) {
			fileName = fileName.replaceAll("[:]", "%3a");
		}
		return fileName;
	}

	/**
	 * revert [:] into [%3a] for file name
	 * 
	 * @param fileName
	 * @return reverted filename
	 */
	String revertInvalidChar(String fileName) {
		if (fileName.indexOf("%3a") > 0) {
			fileName = fileName.replaceAll("[%]3a", ":");
		}
		return fileName;
	}

	/**
	 * Checks if the user has a read permission on the object
	 * 
	 * @return true if the user can read the object
	 */
	boolean hasReadPermission(FileStatus fileStatus) {
		FsPermission permissions = fileStatus.getPermission();
		boolean hasRead = false;
		if (permissions.getOtherAction().equals(FsAction.READ_WRITE)
				|| permissions.getOtherAction().equals(FsAction.READ)
				|| permissions.getOtherAction().equals(FsAction.READ_EXECUTE)
				|| permissions.getOtherAction().equals(FsAction.ALL))
			hasRead = true;
		else if (userName.equals(fileStatus.getOwner())
				&& (permissions.getUserAction().equals(FsAction.READ_WRITE)
						|| permissions.getUserAction().equals(FsAction.READ)
						|| permissions.getUserAction().equals(
								FsAction.READ_EXECUTE) || permissions
						.getUserAction().equals(FsAction.ALL)))
			hasRead = true;
		else if (isGroupMember(fileStatus.getGroup(), groupNames)
				&& (permissions.getGroupAction().equals(FsAction.READ_WRITE)
						|| permissions.getGroupAction().equals(FsAction.READ)
						|| permissions.getGroupAction().equals(
								FsAction.READ_EXECUTE) || permissions
						.getGroupAction().equals(FsAction.ALL)))
			hasRead = true;
		if (hasRead)
			log.debug("PERMISSIONS: " + fileStatus.getPath().toString() + " - "
					+ " read allowed for current process");
		return hasRead;
	}

	/**
	 * Checks if the user has a execute permission on the object
	 * 
	 * @return true if the user can execute the object
	 */
	boolean hasExecutePermission(FileStatus fileStatus) {
		boolean hasExcute = false;
		FsPermission permissions = fileStatus.getPermission();
		if (permissions.getOtherAction().equals(FsAction.READ_EXECUTE)
				|| permissions.getOtherAction().equals(FsAction.ALL))
			hasExcute = true;
		else if (userName.equals(fileStatus.getOwner())
				&& (permissions.getUserAction().equals(FsAction.READ_EXECUTE) || permissions
						.getUserAction().equals(FsAction.ALL)))
			hasExcute = true;
		else if (isGroupMember(fileStatus.getGroup(), groupNames)
				&& (permissions.getGroupAction().equals(FsAction.READ_EXECUTE) || permissions
						.getGroupAction().equals(FsAction.ALL)))
			hasExcute = true;
		if (hasExcute)
			log.debug("PERMISSIONS: " + fileStatus.getPath().toString() + " - "
					+ " execute allowed for current process");
		return hasExcute;
	}

	/**
	 * Checks if user is a member of the group
	 * 
	 * @param group
	 *            to check
	 * @return true if the user id a member of the group
	 */
	boolean isGroupMember(String group, String[] groups) {
		for (String userGroup : groups) {
			if (userGroup.equals(group)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Return an abbreviated English-language desc of the byte length
	 */
	static String byteDesc(long len) {
		double val = 0.0;
		String ending = "";
		if (len < 1024) {
			val = len;
		} else if (len < 1024 * 1024) {
			val = (1.0 * len) / 1024;
			ending = "K";
		} else if (len < 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024 * 1024);
			ending = "M";
		} else if (len < 1024L * 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024 * 1024 * 1024);
			ending = "G";
		} else if (len < 1024L * 1024 * 1024 * 1024 * 1024) {
			val = (1.0 * len) / (1024L * 1024 * 1024 * 1024);
			ending = "T";
		} else {
			val = (1.0 * len) / (1024L * 1024 * 1024 * 1024 * 1024);
			ending = "P";
		}
		return limitDecimalTo2(val) + ending;
	}

	/**
	 * format a number to string
	 * 
	 * @param d
	 *            number
	 * @return formatted number
	 */
	static synchronized String limitDecimalTo2(double d) {
		return decimalFormat.format(d);
	}

}
