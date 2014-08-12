/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3native;

import static org.apache.hadoop.fs.s3native.NativeS3FileSystem.PATH_DELIMITER;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.jets3t.service.S3ServiceException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AmazonS3ClientNativeFileSystemStore implements
        NativeFileSystemStore {

    private AmazonS3Client s3Client;
    private Bucket bucket;

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {

        this.bucket = new Bucket(uri.getHost());

        S3Credentials s3Credentials = new S3Credentials();
        s3Credentials.initialize(uri, conf);

        String accessKey = s3Credentials.getAccessKey();
        String secretAccessKey = s3Credentials.getSecretAccessKey();

        // if credentials are set explicitly
        if (accessKey != null && secretAccessKey != null
                && !accessKey.equals("") && !secretAccessKey.equals("")) {
            // export chosen credentials to java properties
            System.setProperty("aws.accessKeyId", accessKey);
            System.setProperty("aws.secretKey", secretAccessKey);
        }

        // call default constructor to choose properties by role or explicitly
        this.s3Client = new AmazonS3Client();
    }

    @Override
    public void storeFile(String key, File file, byte[] md5Hash)
            throws IOException {
        /*
         * ignores md5hash, the check is handled by AmazonS3Client#putObject(..)
         */
        try {
            this.s3Client.putObject(bucket.getName(), key, file);
        } catch (AmazonClientException e) {
            /*
             * catching amazon specific exceptions and map to interface to avoid
             * leaking AWS functionality
             */
            throw new IOException(e);
        }
    }

    @Override
    public void storeEmptyFile(String key) throws IOException {
        // create empty stream for empty file upload
        DataInputStream emptyStream = new DataInputStream(
                new ByteArrayInputStream(new byte[0]));
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("binary/octet-stream");
        metadata.setContentLength(0);
        this.s3Client.putObject(this.bucket.getName(), key, emptyStream,
                metadata);
    }

    @Override
    public FileMetadata retrieveMetadata(String key) throws IOException {
        try {
            ObjectMetadata objectMetadata = this.s3Client.getObjectMetadata(
                    this.bucket.getName(), key);
            return new FileMetadata(key, objectMetadata.getContentLength(),
                    objectMetadata.getLastModified().getTime());
        } catch (AmazonServiceException e) {
            if (e.getStatusCode() == 404) {
                // ignore 404 similar to Jets3t implementation
                return null;
            }
            throw e;
        }
    }

    @Override
    public InputStream retrieve(String key) throws IOException {
        return this.s3Client.getObject(this.bucket.getName(), key)
                .getObjectContent();
    }

    @Override
    public InputStream retrieve(String key, long byteRangeStart)
            throws IOException {
        InputStream is = this.s3Client.getObject(this.bucket.getName(), key)
                .getObjectContent();
        is.skip(byteRangeStart);
        return is;
    }

    @Override
    public PartialListing list(String prefix, int maxListingLength)
            throws IOException {

        return list(prefix, maxListingLength, null, false);
    }

    @Override
    public PartialListing list(String prefix, int maxListingLength,
            String priorLastKey, boolean recurse) throws IOException {

        return list(prefix, recurse ? null : PATH_DELIMITER, maxListingLength,
                priorLastKey);
    }

    private PartialListing list(String prefix, String delimiter,
            int maxListingLength, String priorLastKey) throws IOException {

        if (prefix.length() > 0 && !prefix.endsWith(PATH_DELIMITER)) {
            prefix += PATH_DELIMITER;
        }

        ListObjectsRequest listObjectRequest = new ListObjectsRequest(
                this.bucket.getName(), prefix, priorLastKey, delimiter,
                maxListingLength);

        ObjectListing listObjects = this.s3Client
                .listObjects(listObjectRequest);

        List<S3ObjectSummary> objectSummaries = listObjects
                .getObjectSummaries();

        FileMetadata[] fileMetadata = new FileMetadata[objectSummaries.size()];
        for (int i = 0; i < fileMetadata.length; i++) {
            S3ObjectSummary objectSummary = objectSummaries.get(i);
            fileMetadata[i] = new FileMetadata(objectSummary.getKey(),
                    objectSummary.getSize(), objectSummary.getLastModified()
                            .getTime());
        }

        return new PartialListing(priorLastKey, fileMetadata, listObjects
                .getCommonPrefixes().toArray(new String[0]));
    }

    @Override
    public void delete(String key) throws IOException {
        this.s3Client.deleteObject(this.bucket.getName(), key);
    }

    @Override
    public void copy(String srcKey, String dstKey) throws IOException {
        this.s3Client.copyObject(this.bucket.getName(), srcKey,
                this.bucket.getName(), dstKey);
    }

    @Override
    public void purge(String prefix) throws IOException {
        try {
            ObjectListing listObjects = this.s3Client.listObjects(
                    this.bucket.getName(), prefix);

            for (S3ObjectSummary objectSummary : listObjects
                    .getObjectSummaries()) {
                this.s3Client.deleteObject(objectSummary.getBucketName(),
                        objectSummary.getKey());
            }
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }

    }

    @Override
    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("S3 Native Filesystem, ");
        sb.append(this.bucket.getName()).append("\n");
        try {
            ObjectListing listObjects = this.s3Client.listObjects(this.bucket
                    .getName());
            for (S3ObjectSummary objectSummary : listObjects
                    .getObjectSummaries()) {
                sb.append(objectSummary.getKey()).append("\n");
            }
        } catch (AmazonClientException e) {
            throw new IOException(e);
        }
        System.out.println(sb);
    }

}
