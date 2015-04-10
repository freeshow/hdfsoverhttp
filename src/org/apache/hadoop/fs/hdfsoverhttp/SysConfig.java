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
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.ServletContext;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class SysConfig {

	static Logger log = Logger.getLogger(SysConfig.class);

	private static Properties props;
	private static String CONF_FILE = "hdfs-over-http.conf";
	static String ROOT_DIR;
	static String HDFS_URI;
	static String INDEX_HTML;
	public static String README_FILE;
	public static String HEADER_FILE;
	public static boolean hasReadme;
	public static boolean hasHeader;

	/**
	 * get parameter from the config file
	 * 
	 * @param context
	 */
	static public void init(ServletContext context) {
		if (props == null) {
			try {
				props = getProps(context);
				ROOT_DIR = props.getProperty("root-dir", "/").trim();
				if (ROOT_DIR.endsWith(Path.SEPARATOR)) {
					ROOT_DIR = ROOT_DIR.substring(0, ROOT_DIR.length() - 1);
				}

				HDFS_URI = props.getProperty("hdfs-uri").trim();

				INDEX_HTML = props.getProperty("DirectoryIndex", "index.html")
						.trim();

				README_FILE = props.getProperty("ReadmeName").trim();
				if (README_FILE != null)
					hasReadme = true;
				HEADER_FILE = props.getProperty("HeaderName", "").trim();
				if (HEADER_FILE != null)
					hasHeader = true;

			} catch (IOException e) {
				log.error(CONF_FILE + " doesn't exist", e);
			} catch (Exception e) {
				log.error(e);
			}
		}
	}

	/**
	 * load configure file
	 * 
	 * @param context
	 * @return object of property
	 * @throws IOException
	 */
	static private Properties getProps(ServletContext context)
			throws IOException {
		Properties props = new Properties();
		InputStream inputStream = context.getResourceAsStream("/WEB-INF/conf/"
				+ CONF_FILE);
		props.load(inputStream);
		return props;
	}

}