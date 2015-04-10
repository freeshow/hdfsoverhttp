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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.FileStatus;

public class Range {
	/**
	 * HTTP date format.
	 */
	protected static final SimpleDateFormat format = new SimpleDateFormat(
			"EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
	protected final static TimeZone gmtZone = TimeZone.getTimeZone("GMT");

	/**
	 * GMT timezone - all HTTP dates are on GMT
	 */
	static {
		format.setTimeZone(gmtZone);
	}

	/**
	 * Full range marker.
	 */
	protected static ArrayList<Range> FULL = new ArrayList<Range>();

	public long start;
	public long end;
	public long length;

	/**
	 * Validate range.
	 */
	public boolean validate() {
		if (end >= length)
			end = length - 1;
		return ((start >= 0) && (end >= 0) && (start <= end) && (length > 0));
	}

	public void recycle() {
		start = 0;
		end = 0;
		length = 0;
	}

	/**
	 * Get ETag.
	 * 
	 * @return strong ETag if available, else weak ETag.
	 */
	public static String getETag(FileStatus fileStatus) {
		String weakETag = null;
		long contentLength = fileStatus.getLen();
		long lastModified = fileStatus.getModificationTime();
		if ((contentLength >= 0) || (lastModified >= 0)) {
			weakETag = "W/\"" + contentLength + "-" + lastModified + "\"";
		}
		return weakETag;
	}

	/**
	 * Parse the range header.
	 * 
	 * @param request
	 *            The servlet request we are processing
	 * @param response
	 *            The servlet response we are creating
	 * @return Vector of ranges
	 */
	protected ArrayList<Range> parseRange(HttpServletRequest request,
			HttpServletResponse response, FileStatus fileStatus)
			throws IOException {

		// Checking If-Range
		String headerValue = request.getHeader("If-Range");

		if (headerValue != null) {

			long headerValueTime = (-1L);
			try {
				headerValueTime = request.getDateHeader("If-Range");
			} catch (IllegalArgumentException e) {
				;
			}

			String eTag = getETag(fileStatus);
			long lastModified = fileStatus.getModificationTime();

			if (headerValueTime == (-1L)) {

				// If the ETag the client gave does not match the entity
				// etag, then the entire entity is returned.
				if (!eTag.equals(headerValue.trim()))
					return FULL;

			} else {

				// If the timestamp of the entity the client got is older than
				// the last modification date of the entity, the entire entity
				// is returned.
				if (lastModified > (headerValueTime + 1000))
					return FULL;

			}

		}

		long fileLength = fileStatus.getLen();

		if (fileLength == 0)
			return null;

		// Retrieving the range header (if any is specified
		String rangeHeader = request.getHeader("Range");

		if (rangeHeader == null)
			return null;
		// bytes is the only range unit supported (and I don't see the point
		// of adding new ones).
		if (!rangeHeader.startsWith("bytes")) {
			response.addHeader("Content-Range", "bytes */" + fileLength);
			response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
			return null;
		}

		rangeHeader = rangeHeader.substring(6);

		// Vector which will contain all the ranges which are successfully
		// parsed.
		ArrayList<Range> result = new ArrayList<Range>();
		StringTokenizer commaTokenizer = new StringTokenizer(rangeHeader, ",");

		// Parsing the range list
		while (commaTokenizer.hasMoreTokens()) {
			String rangeDefinition = commaTokenizer.nextToken().trim();

			Range currentRange = new Range();
			currentRange.length = fileLength;

			int dashPos = rangeDefinition.indexOf('-');

			if (dashPos == -1) {
				response.addHeader("Content-Range", "bytes */" + fileLength);
				response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
				return null;
			}

			if (dashPos == 0) {

				try {
					long offset = Long.parseLong(rangeDefinition);
					currentRange.start = fileLength + offset;
					currentRange.end = fileLength - 1;
				} catch (NumberFormatException e) {
					response.addHeader("Content-Range", "bytes */" + fileLength);
					response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
					return null;
				}

			} else {

				try {
					currentRange.start = Long.parseLong(rangeDefinition
							.substring(0, dashPos));
					if (dashPos < rangeDefinition.length() - 1)
						currentRange.end = Long.parseLong(rangeDefinition
								.substring(dashPos + 1,
										rangeDefinition.length()));
					else
						currentRange.end = fileLength - 1;
				} catch (NumberFormatException e) {
					response.addHeader("Content-Range", "bytes */" + fileLength);
					response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
					return null;
				}

			}

			if (!currentRange.validate()) {
				response.addHeader("Content-Range", "bytes */" + fileLength);
				response.sendError(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
				return null;
			}

			result.add(currentRange);
		}

		return result;
	}

	/**
	 * @return Returns the lastModifiedHttp.
	 */
	public static String getLastModifiedHttp(FileStatus fileStatus) {
		String lastModifiedHttp = null;
		synchronized (format) {
			lastModifiedHttp = format.format(new Date(fileStatus
					.getModificationTime()));
		}
		return lastModifiedHttp;
	}
}