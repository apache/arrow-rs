// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cloud Multipart Upload
//!
//! This crate provides an asynchronous interface for multipart file uploads to cloud storage services.
//! It's designed to offer efficient, non-blocking operations,
//! especially useful when dealing with large files or high-throughput systems.

use async_trait::async_trait;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use std::{io, pin::Pin, sync::Arc, task::Poll};
use tokio::io::AsyncWrite;

use crate::Result;

type BoxedTryFuture<T> = Pin<Box<dyn Future<Output = Result<T, io::Error>> + Send>>;

/// A trait that can be implemented by cloud-based object stores
/// and used in combination with [`CloudMultiPartUpload`] to provide
/// multipart upload support
#[async_trait]
pub trait CloudMultiPartUploadImpl: 'static {
    /// Upload a single part
    async fn put_multipart_part(
        &self,
        buf: Vec<u8>,
        part_idx: usize,
    ) -> Result<UploadPart, io::Error>;

    /// Complete the upload with the provided parts
    ///
    /// `completed_parts` is in order of part number
    async fn complete(&self, completed_parts: Vec<UploadPart>) -> Result<(), io::Error>;
}

/// Represents a part of a file that has been successfully uploaded in a multipart upload process.
#[derive(Debug, Clone)]
pub struct UploadPart {
    /// Id of this part
    pub content_id: String,
}

/// Struct that manages and controls multipart uploads to a cloud storage service.
pub struct CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl,
{
    inner: Arc<T>,
    /// A list of completed parts, in sequential order.
    completed_parts: Vec<Option<UploadPart>>,
    /// Part upload tasks currently running
    tasks: FuturesUnordered<BoxedTryFuture<(usize, UploadPart)>>,
    /// Maximum number of upload tasks to run concurrently
    max_concurrency: usize,
    /// Buffer that will be sent in next upload.
    current_buffer: Vec<u8>,
    /// Size of each part.
    ///
    /// While S3 and Minio support variable part sizes, R2 requires they all be
    /// exactly the same size.
    part_size: usize,
    /// Index of current part
    current_part_idx: usize,
    /// The completion task
    completion_task: Option<BoxedTryFuture<()>>,
}

impl<T> CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl,
{
    /// Create a new multipart upload with the implementation and the given maximum concurrency
    pub fn new(inner: T, max_concurrency: usize) -> Self {
        Self {
            inner: Arc::new(inner),
            completed_parts: Vec::new(),
            tasks: FuturesUnordered::new(),
            max_concurrency,
            current_buffer: Vec::new(),
            // TODO: Should self vary by provider?
            // TODO: Should we automatically increase then when part index gets large?

            // Minimum size of 5 MiB
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
            // https://cloud.google.com/storage/quotas#requests
            part_size: 10 * 1024 * 1024,
            current_part_idx: 0,
            completion_task: None,
        }
    }

    // Add data to the current buffer, returning the number of bytes added
    fn add_to_buffer(mut self: Pin<&mut Self>, buf: &[u8], offset: usize) -> usize {
        let remaining_capacity = self.part_size - self.current_buffer.len();
        let to_copy = std::cmp::min(remaining_capacity, buf.len() - offset);
        self.current_buffer
            .extend_from_slice(&buf[offset..offset + to_copy]);
        to_copy
    }

    /// Poll current tasks
    pub fn poll_tasks(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Result<(), io::Error> {
        if self.tasks.is_empty() {
            return Ok(());
        }
        while let Poll::Ready(Some(res)) = self.tasks.poll_next_unpin(cx) {
            let (part_idx, part) = res?;
            let total_parts = self.completed_parts.len();
            self.completed_parts
                .resize(std::cmp::max(part_idx + 1, total_parts), None);
            self.completed_parts[part_idx] = Some(part);
        }
        Ok(())
    }
}

impl<T> CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl + Send + Sync,
{
    // The `poll_flush` function will only flush the in-progress tasks.
    // The `final_flush` method called during `poll_shutdown` will flush
    // the `current_buffer` along with in-progress tasks.
    // Please see https://github.com/apache/arrow-rs/issues/3390 for more details.
    fn final_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // Poll current tasks
        self.as_mut().poll_tasks(cx)?;

        // If current_buffer is not empty, see if it can be submitted
        if !self.current_buffer.is_empty() && self.tasks.len() < self.max_concurrency {
            let out_buffer: Vec<u8> = std::mem::take(&mut self.current_buffer);
            let inner = Arc::clone(&self.inner);
            let part_idx = self.current_part_idx;
            self.tasks.push(Box::pin(async move {
                let upload_part = inner.put_multipart_part(out_buffer, part_idx).await?;
                Ok((part_idx, upload_part))
            }));
        }

        self.as_mut().poll_tasks(cx)?;

        // If tasks and current_buffer are empty, return Ready
        if self.tasks.is_empty() && self.current_buffer.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl<T> AsyncWrite for CloudMultiPartUpload<T>
where
    T: CloudMultiPartUploadImpl + Send + Sync,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        // Poll current tasks
        self.as_mut().poll_tasks(cx)?;

        let mut offset = 0;

        loop {
            // Fill up current buffer
            offset += self.as_mut().add_to_buffer(buf, offset);

            // If we don't have a full buffer or we have too many tasks, break
            if self.current_buffer.len() < self.part_size
                || self.tasks.len() >= self.max_concurrency
            {
                break;
            }

            let new_buffer = Vec::with_capacity(self.part_size);
            let out_buffer = std::mem::replace(&mut self.current_buffer, new_buffer);
            let inner = Arc::clone(&self.inner);
            let part_idx = self.current_part_idx;
            self.tasks.push(Box::pin(async move {
                let upload_part = inner.put_multipart_part(out_buffer, part_idx).await?;
                Ok((part_idx, upload_part))
            }));
            self.current_part_idx += 1;

            // We need to poll immediately after adding to setup waker
            self.as_mut().poll_tasks(cx)?;
        }

        // If offset is zero, then we didn't write anything because we didn't
        // have capacity for more tasks and our buffer is full.
        if offset == 0 && !buf.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(offset))
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // Poll current tasks
        self.as_mut().poll_tasks(cx)?;

        // If tasks is empty, return Ready
        if self.tasks.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        // First, poll flush
        match self.as_mut().final_flush(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(res) => res?,
        };

        // If shutdown task is not set, set it
        let parts = std::mem::take(&mut self.completed_parts);
        let parts = parts
            .into_iter()
            .enumerate()
            .map(|(idx, part)| {
                part.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Missing information for upload part {idx}"),
                    )
                })
            })
            .collect::<Result<_, _>>()?;

        let inner = Arc::clone(&self.inner);
        let completion_task = self.completion_task.get_or_insert_with(|| {
            Box::pin(async move {
                inner.complete(parts).await?;
                Ok(())
            })
        });

        Pin::new(completion_task).poll(cx)
    }
}

impl<T: CloudMultiPartUploadImpl> std::fmt::Debug for CloudMultiPartUpload<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudMultiPartUpload")
            .field("completed_parts", &self.completed_parts)
            .field("tasks", &self.tasks)
            .field("max_concurrency", &self.max_concurrency)
            .field("current_buffer", &self.current_buffer)
            .field("part_size", &self.part_size)
            .field("current_part_idx", &self.current_part_idx)
            .finish()
    }
}
