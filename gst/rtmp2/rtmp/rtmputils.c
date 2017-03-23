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
 * Free Software Foundation, Inc., 51 Franklin Street, Suite 500,
 * Boston, MA 02110-1335, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/gst.h>

#include "rtmputils.h"
#include "rtmpdebug.h"

#include <string.h>

static void read_all_bytes_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void write_all_bytes_done (GObject * source, GAsyncResult * result,
    gpointer user_data);

void
gst_rtmp_dump_bytes (const gchar * string, GBytes * bytes)
{
  if (G_UNLIKELY (GST_LEVEL_MEMDUMP <= _gst_debug_min) && GST_LEVEL_MEMDUMP <=
      gst_debug_category_get_threshold (GST_CAT_DEFAULT)) {
    gsize size;
    const guint8 *data = g_bytes_get_data (bytes, &size);
    GST_MEMDUMP (string, data, size);
  }
}

void
gst_rtmp_byte_array_append_bytes (GByteArray * bytearray, GBytes * bytes)
{
  const guint8 *data;
  gsize size;
  guint offset;

  g_return_if_fail (bytearray);

  offset = bytearray->len;
  data = g_bytes_get_data (bytes, &size);

  g_return_if_fail (data);

  g_byte_array_set_size (bytearray, offset + size);
  memcpy (bytearray->data + offset, data, size);
}

void
gst_rtmp_input_stream_read_all_bytes_async (GInputStream * stream, gsize count,
    int io_priority, GCancellable * cancellable, GAsyncReadyCallback callback,
    gpointer user_data)
{
  GTask *task;
  GByteArray *ba;

  g_return_if_fail (G_IS_INPUT_STREAM (stream));

  task = g_task_new (stream, cancellable, callback, user_data);

  ba = g_byte_array_sized_new (count);
  g_byte_array_set_size (ba, count);
  g_task_set_task_data (task, ba, (GDestroyNotify) g_byte_array_unref);

  g_input_stream_read_all_async (stream, ba->data, count, io_priority,
      cancellable, read_all_bytes_done, task);
}

static void
read_all_bytes_done (GObject * source, GAsyncResult * result,
    gpointer user_data)
{
  GInputStream *is = G_INPUT_STREAM (source);
  GTask *task = user_data;
  GByteArray *ba = g_task_get_task_data (task);
  GError *error = NULL;
  gboolean res;
  gsize bytes_read;
  GBytes *bytes;

  res = g_input_stream_read_all_finish (is, result, &bytes_read, &error);
  if (!res) {
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  g_byte_array_set_size (ba, bytes_read);
  bytes = g_byte_array_free_to_bytes (g_byte_array_ref (ba));

  g_task_return_pointer (task, bytes, (GDestroyNotify) g_bytes_unref);
  g_object_unref (task);
}

GBytes *
gst_rtmp_input_stream_read_all_bytes_finish (GInputStream * stream,
    GAsyncResult * result, GError ** error)
{
  g_return_val_if_fail (g_task_is_valid (result, stream), FALSE);
  return g_task_propagate_pointer (G_TASK (result), error);
}

void
gst_rtmp_output_stream_write_all_bytes_async (GOutputStream * stream,
    GBytes * bytes, int io_priority, GCancellable * cancellable,
    GAsyncReadyCallback callback, gpointer user_data)
{
  GTask *task;
  const void *data;
  gsize size;

  g_return_if_fail (G_IS_OUTPUT_STREAM (stream));
  g_return_if_fail (bytes);

  data = g_bytes_get_data (bytes, &size);
  g_return_if_fail (data);

  task = g_task_new (stream, cancellable, callback, user_data);
  g_task_set_task_data (task, g_bytes_ref (bytes),
      (GDestroyNotify) g_bytes_unref);

  g_output_stream_write_all_async (stream, data, size, io_priority,
      cancellable, write_all_bytes_done, task);
}

static void
write_all_bytes_done (GObject * source, GAsyncResult * result,
    gpointer user_data)
{
  GOutputStream *os = G_OUTPUT_STREAM (source);
  GTask *task = user_data;
  GError *error = NULL;
  gboolean res;

  res = g_output_stream_write_all_finish (os, result, NULL, &error);
  if (!res) {
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  g_task_return_boolean (task, TRUE);
  g_object_unref (task);
}

gboolean
gst_rtmp_output_stream_write_all_bytes_finish (GOutputStream * stream,
    GAsyncResult * result, GError ** error)
{
  g_return_val_if_fail (g_task_is_valid (result, stream), FALSE);
  return g_task_propagate_boolean (G_TASK (result), error);
}
