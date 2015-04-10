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

public class DirectoryMetaInfo {

	// the max length of file name in file list
	private int fileNameMaxLength = 25;

	public int getFileNameMaxLength() {
		return fileNameMaxLength;
	}

	public void setFileNameMaxLength(int fileNameMaxLength) {
		if (fileNameMaxLength > this.fileNameMaxLength)
			this.fileNameMaxLength = fileNameMaxLength;
	}

	public void setFileNameMaxLength(int fileNameBytesLength,
			int fileNameCharLength) {
		int len = fileNameCharLength;
		if (fileNameBytesLength > fileNameCharLength) {
			len = fileNameBytesLength
					- (fileNameBytesLength - fileNameCharLength) / 2;
		}
		if (len > this.fileNameMaxLength)
			this.fileNameMaxLength = len;
	}

	// the max length of file's size column
	private int fileSizeMaxLength = 1;

	public int getFileSizeMaxLength() {
		return fileSizeMaxLength;
	}

	public void setFileSizeMaxLength(int fileSizeMaxLength) {
		if (fileSizeMaxLength > this.fileSizeMaxLength)
			this.fileSizeMaxLength = fileSizeMaxLength;
	}

	// the flag of readme file exist or doesn't exist
	private boolean readmeExist;

	public boolean isReadmeExist() {
		return readmeExist;
	}

	public void setReadmeExist(boolean readmeExist) {
		this.readmeExist = readmeExist;
	}

	// the flag of header file exist or doesn't exist
	private boolean headerExist;

	public boolean isHeaderExist() {
		return headerExist;
	}

	public void setHeaderExist(boolean headerExist) {
		this.headerExist = headerExist;
	}

	/**
	 * create blank string by number
	 * 
	 * @param len
	 * @return blank string
	 */
	static public String createBlankTag(int len) {
		char[] blankCharArr = new char[len];
		for (int i = 0; i < len; i++) {
			blankCharArr[i] = ' ';
		}
		return String.valueOf(blankCharArr);
	}

	/**
	 * create blank string
	 * 
	 * @param fileNameMaxLength
	 * @param fileNameBytesLength
	 * @param fileNameCharLength
	 * @return blank string
	 */
	static public String createBlankTag(int fileNameMaxLength,
			int fileNameBytesLength, int fileNameCharLength) {
		int len = fileNameMaxLength - fileNameCharLength;
		if (fileNameBytesLength > fileNameCharLength) {
			len = len - (fileNameBytesLength - fileNameCharLength) / 2;
		}
		return createBlankTag(len);
	}

}