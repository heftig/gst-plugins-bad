/* GStreamer RTMP Library
 * Copyright (C) 2017 Make.TV, Inc. <info@make.tv>
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

#include "rtmpinputstream.h"
#include "rtmpchunkstream.h"
#include "rtmpmessage.h"
#include "rtmputils.h"

#define GST_CAT_DEFAULT gst_rtmp_input_stream_debug_category
GST_DEBUG_CATEGORY_STATIC (GST_CAT_DEFAULT);

typedef void (*TaskFunc) (GTask * task);

typedef struct
{
  GstRtmpChunkStream *cstream;
  TaskFunc next_func;
  gsize needed;
} ReadData;

static ReadData *
read_data_new (void)
{
  ReadData *data = g_slice_new0 (ReadData);
  return data;
}

static void
read_data_free (gpointer ptr)
{
  ReadData *data = ptr;
  g_slice_free (ReadData, data);
}

enum
{
  PROP_0,

  PROP_CHUNK_SIZE,
  PROP_ACK_WINDOW_SIZE,
  PROP_BYTES,
  PROP_BYTES_ACKED,

  N_PROPERTIES
};

static GParamSpec *properties[N_PROPERTIES] = { NULL, };

enum
{
  SIGNAL_NEED_ACK,

  N_SIGNALS
};

static guint signals[N_SIGNALS] = { 0, };

struct _GstRtmpInputStream
{
  GBufferedInputStream parent_instance;
  GstRtmpChunkStreams *chunk_streams;
  guint chunk_size, ack_window_size;
  guint64 bytes, bytes_acked;
};

typedef struct
{
  GBufferedInputStreamClass parent_class;
} GstRtmpInputStreamClass;

G_DEFINE_TYPE (GstRtmpInputStream, gst_rtmp_input_stream,
    G_TYPE_BUFFERED_INPUT_STREAM);

static void gst_rtmp_input_stream_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec);
static void gst_rtmp_input_stream_finalize (GObject * object);
static void read_chunk_header (GTask * task);
static void read_payload (GTask * task);
static void return_message (GTask * task);
static void continue_read (GTask * task);
static void fill_done (GObject * source_object, GAsyncResult * result,
    gpointer user_data);


static void
gst_rtmp_input_stream_class_init (GstRtmpInputStreamClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_class->get_property = gst_rtmp_input_stream_get_property;
  gobject_class->finalize = gst_rtmp_input_stream_finalize;

  properties[PROP_CHUNK_SIZE] = g_param_spec_uint ("chunk-size", "Chunk size",
      "RTMP chunk size", 1, 0x7fffffff, GST_RTMP_DEFAULT_CHUNK_SIZE,
      G_PARAM_READABLE | G_PARAM_STATIC_STRINGS);

  properties[PROP_ACK_WINDOW_SIZE] = g_param_spec_uint ("ack-window-size",
      "Ack window size", "Acknowledgement window size", 0, G_MAXUINT, 0,
      G_PARAM_READABLE | G_PARAM_STATIC_STRINGS);

  properties[PROP_BYTES] = g_param_spec_uint64 ("bytes", "Bytes",
      "Raw bytes transferred", 0, G_MAXUINT64, 0,
      G_PARAM_READABLE | G_PARAM_STATIC_STRINGS);

  properties[PROP_BYTES_ACKED] = g_param_spec_uint64 ("bytes-acked",
      "Bytes acked", "Raw bytes acknowledged", 0, G_MAXUINT64, 0,
      G_PARAM_READABLE | G_PARAM_STATIC_STRINGS);

  g_object_class_install_properties (gobject_class, N_PROPERTIES, properties);

  signals[SIGNAL_NEED_ACK] = g_signal_new ("need-ack",
      G_TYPE_FROM_CLASS (klass), G_SIGNAL_RUN_LAST, 0, NULL, NULL, NULL,
      G_TYPE_NONE, 0);

  GST_DEBUG_CATEGORY_INIT (GST_CAT_DEFAULT, "rtmpinputstream", 0,
      "debug category for GstRtmpInputStream");

  GST_DEBUG_REGISTER_FUNCPTR (read_chunk_header);
  GST_DEBUG_REGISTER_FUNCPTR (read_payload);
}

GstRtmpInputStream *
gst_rtmp_input_stream_new (GInputStream * base_stream)
{
  GstRtmpInputStream *stream;

  g_return_val_if_fail (G_IS_INPUT_STREAM (base_stream), NULL);

  stream = g_object_new (GST_TYPE_RTMP_INPUT_STREAM,
      "base-stream", base_stream, NULL);

  return stream;
}

static gboolean
calc_buffer_size (GBinding * binding, const GValue * from, GValue * to,
    gpointer user_data)
{
  guint chunk_size, buffer_size;

  chunk_size = g_value_get_uint (from);
  buffer_size = MAX (4096, 2 * chunk_size);

  g_value_set_uint (to, buffer_size);
  return TRUE;
}

static void
gst_rtmp_input_stream_init (GstRtmpInputStream * self)
{
  self->chunk_size = GST_RTMP_DEFAULT_CHUNK_SIZE;
  self->chunk_streams = gst_rtmp_chunk_streams_new ();

  g_object_bind_property_full (self, "chunk-size", self, "buffer-size",
      G_BINDING_SYNC_CREATE, calc_buffer_size, NULL, NULL, NULL);
}

static void
gst_rtmp_input_stream_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec)
{
  GstRtmpInputStream *self = GST_RTMP_INPUT_STREAM (object);

  switch (property_id) {
    case PROP_CHUNK_SIZE:
      g_value_set_uint (value, self->chunk_size);
      break;
    case PROP_ACK_WINDOW_SIZE:
      g_value_set_uint (value, self->ack_window_size);
      break;
    case PROP_BYTES:
      g_value_set_uint64 (value, self->bytes);
      break;
    case PROP_BYTES_ACKED:
      g_value_set_uint64 (value, self->bytes_acked);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

static void
gst_rtmp_input_stream_finalize (GObject * object)
{
  GstRtmpInputStream *self = GST_RTMP_INPUT_STREAM (object);

  g_clear_pointer (&self->chunk_streams, gst_rtmp_chunk_streams_free);

  G_OBJECT_CLASS (gst_rtmp_input_stream_parent_class)->finalize (object);
}

static void
start_read (GTask * task, gsize needed, TaskFunc next_func)
{
  ReadData *task_data = g_task_get_task_data (task);

  GST_LOG ("Starting read for %s with %" G_GSIZE_FORMAT " bytes needed",
      GST_DEBUG_FUNCPTR_NAME (next_func), needed);

  task_data->next_func = next_func;
  task_data->needed = needed;

  continue_read (task);
}

static void
continue_read (GTask * task)
{
  GBufferedInputStream *bstream = g_task_get_source_object (task);
  ReadData *task_data = g_task_get_task_data (task);
  TaskFunc next_func = task_data->next_func;
  gsize available, needed;

  available = g_buffered_input_stream_get_available (bstream);
  needed = task_data->needed;

  if (available < needed) {
    needed -= available;

    GST_LOG ("Only %" G_GSIZE_FORMAT " bytes available, need %" G_GSIZE_FORMAT
        " more; filling up", available, needed);
    g_buffered_input_stream_fill_async (bstream, (gssize) needed,
        G_PRIORITY_DEFAULT, g_task_get_cancellable (task), fill_done, task);
    return;
  }

  GST_LOG ("Have %" G_GSIZE_FORMAT " bytes available, needed %" G_GSIZE_FORMAT
      "; calling %s", available, needed, GST_DEBUG_FUNCPTR_NAME (next_func));

  next_func (task);
}

static void
fill_done (GObject * source_object, GAsyncResult * result, gpointer user_data)
{
  GBufferedInputStream *bstream = G_BUFFERED_INPUT_STREAM (source_object);
  GTask *task = user_data;
  GstRtmpInputStream *self = g_task_get_source_object (task);
  GError *error;
  gssize size;

  size = g_buffered_input_stream_fill_finish (bstream, result, &error);
  if (size < 0) {
    GST_ERROR ("Fill failed: %s", error->message);
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  if (size == 0) {
    GST_ERROR ("Fill failed; EOF");
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_CONNECTION_CLOSED,
        "connection was closed");
    g_object_unref (task);
    return;
  }

  if (g_task_return_error_if_cancelled (task)) {
    GST_DEBUG ("Cancelled");
    g_object_unref (task);
    return;
  }

  self->bytes += size;
  GST_LOG ("Fill completed; read %" G_GSIZE_FORMAT " bytes", size);

  continue_read (task);
}

static void
read_chunk_header (GTask * task)
{
  GstRtmpInputStream *self = g_task_get_source_object (task);
  GBufferedInputStream *bstream = G_BUFFERED_INPUT_STREAM (self);
  GInputStream *istream = G_INPUT_STREAM (self);
  ReadData *task_data = g_task_get_task_data (task);
  guint32 chunk_stream_id, header_size, next_size;
  GstRtmpChunkStream *cstream;
  gssize retsize;
  gconstpointer data;
  gsize size;

  data = g_buffered_input_stream_peek_buffer (bstream, &size);
  chunk_stream_id = gst_rtmp_chunk_stream_parse_id (data, size);

  if (!chunk_stream_id) {
    start_read (task, task_data->needed + 1, read_chunk_header);
    return;
  }

  cstream = task_data->cstream = gst_rtmp_chunk_streams_get
      (self->chunk_streams, chunk_stream_id);
  header_size = gst_rtmp_chunk_stream_parse_header (cstream, data, size);

  if (size < header_size) {
    start_read (task, header_size, read_chunk_header);
    return;
  }

  retsize = g_input_stream_skip (istream, header_size, NULL, NULL);
  g_assert (retsize == header_size);

  next_size = gst_rtmp_chunk_stream_parse_payload (cstream, self->chunk_size,
      NULL);
  if (next_size == 0) {
    return_message (task);
    return;
  }

  start_read (task, next_size, read_payload);
}

static void
read_payload (GTask * task)
{
  GstRtmpInputStream *self = g_task_get_source_object (task);
  GInputStream *istream = G_INPUT_STREAM (self);
  ReadData *task_data = g_task_get_task_data (task);
  GstRtmpChunkStream *cstream = task_data->cstream;
  guint8 *data;
  guint32 next_size;
  gssize retsize;

  next_size = gst_rtmp_chunk_stream_parse_payload (cstream, self->chunk_size,
      &data);

  retsize = g_input_stream_read (istream, data, next_size, NULL, NULL);
  g_assert (retsize == next_size);

  next_size = gst_rtmp_chunk_stream_wrote_payload (cstream, self->chunk_size);
  if (next_size == 0) {
    return_message (task);
    return;
  }

  start_read (task, 1, read_chunk_header);
}

static void
handle_set_chunk_size (GstRtmpInputStream * self, GstBuffer * buffer)
{
  guint32 chunk_size;
  gsize size;

  GST_INFO ("Got set chunk size %" GST_PTR_FORMAT, buffer);

  size = gst_buffer_extract (buffer, 0, &chunk_size, sizeof chunk_size);
  g_return_if_fail (size == sizeof chunk_size);

  chunk_size = GUINT32_FROM_BE (chunk_size);
  if (self->chunk_size != chunk_size) {
    GST_INFO ("Changing chunk size from %" G_GUINT32_FORMAT " to %"
        G_GUINT32_FORMAT, self->chunk_size, chunk_size);
    self->chunk_size = chunk_size;
    g_object_notify_by_pspec (G_OBJECT (self), properties[PROP_CHUNK_SIZE]);
  } else {
    GST_INFO ("Chunk size (%" G_GUINT32_FORMAT ") unchanged", chunk_size);
  }
}

static void
handle_window_ack_size (GstRtmpInputStream * self, GstBuffer * buffer)
{
  guint32 ack_window_size;
  gsize size;

  GST_INFO ("Got window ack size %" GST_PTR_FORMAT, buffer);

  size = gst_buffer_extract (buffer, 0, &ack_window_size,
      sizeof ack_window_size);
  g_return_if_fail (size == sizeof ack_window_size);

  ack_window_size = GUINT32_FROM_BE (ack_window_size);
  if (self->ack_window_size != ack_window_size) {
    GST_INFO ("Changing acknowledgment window size from %" G_GUINT32_FORMAT
        " to %" G_GUINT32_FORMAT, self->ack_window_size, ack_window_size);
    self->ack_window_size = ack_window_size;
    g_object_notify_by_pspec (G_OBJECT (self),
        properties[PROP_ACK_WINDOW_SIZE]);
  } else {
    GST_INFO ("Acknowledgement window size (%" G_GUINT32_FORMAT ") unchanged",
        ack_window_size);
  }
}

static void
handle_abort_message (GstRtmpInputStream * self, GstBuffer * buffer)
{
  GstRtmpChunkStream *cstream;
  guint32 stream_id;
  gsize size;

  GST_FIXME ("Got untested abort message %" GST_PTR_FORMAT, buffer);

  size = gst_buffer_extract (buffer, 0, &stream_id, sizeof stream_id);
  g_return_if_fail (size == sizeof stream_id);

  stream_id = GUINT32_FROM_BE (stream_id);
  GST_INFO ("Aborting message on stream %" G_GUINT32_FORMAT, stream_id);

  cstream = gst_rtmp_chunk_streams_get (self->chunk_streams, stream_id);
  gst_rtmp_chunk_stream_clear (cstream);
}

static void
return_message (GTask * task)
{
  GstRtmpInputStream *self = g_task_get_source_object (task);
  ReadData *task_data = g_task_get_task_data (task);
  GstBuffer *buffer;

  buffer = gst_rtmp_chunk_stream_parse_finish (task_data->cstream);

  if (gst_rtmp_message_is_protocol_control (buffer)) {
    GstRtmpMessageType type = gst_rtmp_message_get_type (buffer);
    GST_LOG ("Got a protocol control message");
    switch (type) {
      case GST_RTMP_MESSAGE_TYPE_SET_CHUNK_SIZE:
        handle_set_chunk_size (self, buffer);
        break;

      case GST_RTMP_MESSAGE_TYPE_WINDOW_ACK_SIZE:
        handle_window_ack_size (self, buffer);
        break;

      case GST_RTMP_MESSAGE_TYPE_ABORT_MESSAGE:
        handle_abort_message (self, buffer);
        break;

      default:
        GST_DEBUG ("Not handling PCM %d:%s", type,
            gst_rtmp_message_type_get_nick (type));
        break;
    }
  }

  g_task_return_pointer (task, buffer, (GDestroyNotify) gst_mini_object_unref);
  g_object_unref (task);
  return;
}

void
gst_rtmp_input_stream_read_async (GstRtmpInputStream * self,
    GCancellable * cancellable,
    GAsyncReadyCallback callback, gpointer user_data)
{

  GTask *task = g_task_new (self, cancellable, callback, user_data);
  g_task_set_task_data (task, read_data_new (), read_data_free);
  start_read (task, 1, read_chunk_header);
}

GstBuffer *
gst_rtmp_input_stream_read_finish (GstRtmpInputStream * self,
    GAsyncResult * result, GError ** error)
{
  g_return_val_if_fail (g_task_is_valid (result, self), NULL);
  return g_task_propagate_pointer (G_TASK (result), error);
}
