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

import java.io.File;
import java.util.HashMap;

public class IconRegister {

	private static HashMap<String, String> iconHm;

	/**
	 * get icon file's name
	 * 
	 * @param iconPath
	 * @param fileExt
	 * @return icon file name
	 */
	static public String getIcon(String iconPath, String fileExt) {
		String iconFileName;
		String iconfFileExt;

		if (iconHm == null) {
			File iconDir = new File(iconPath);
			File[] iconFiles = iconDir.listFiles();
			if (iconFiles == null) {
				throw new RuntimeException("can't find the folder of "
						+ iconPath);
			}
			iconHm = new HashMap<String, String>();
			for (int i = 0; i < iconFiles.length; i++) {
				iconFileName = iconFiles[i].getName();
				iconfFileExt = iconFileName.substring(0,
						iconFileName.lastIndexOf("."));
				iconHm.put(iconfFileExt, iconFileName);
			}
		}
		iconFileName = iconHm.get(fileExt);
		if (iconFileName == null)
			iconFileName = "default.gif";
		return iconFileName;
	}
}
