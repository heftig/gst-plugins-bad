/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#ifndef _GST_RTMP_UTILS_H_
#define _GST_RTMP_UTILS_H_

#include <gio/gio.h>

G_BEGIN_DECLS

#define GST_RTMP_DEFAULT_PORT 1935
#define GST_RTMP_DEFAULT_CHUNK_SIZE 128

void gst_rtmp_dump_bytes (const gchar * string, GBytes * bytes);

void gst_rtmp_byte_array_append_bytes (GByteArray * bytearray, GBytes * bytes);

void gst_rtmp_input_stream_read_all_bytes_async (GInputStream * stream,
    gsize count, int io_priority, GCancellable * cancellable,
    GAsyncReadyCallback callback, gpointer user_data);
GBytes * gst_rtmp_input_stream_read_all_bytes_finish (GInputStream * stream,
    GAsyncResult * result, GError ** error);

void gst_rtmp_output_stream_write_all_bytes_async (GOutputStream * stream,
    GBytes * bytes, int io_priority, GCancellable * cancellable,
    GAsyncReadyCallback callback, gpointer user_data);
gboolean gst_rtmp_output_stream_write_all_bytes_finish (GOutputStream * stream,
    GAsyncResult * result, GError ** error);

G_END_DECLS

#endif
