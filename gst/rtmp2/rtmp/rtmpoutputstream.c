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

#include "rtmpoutputstream.h"
#include "rtmpchunkstream.h"
#include "rtmpmessage.h"
#include "rtmputils.h"

#define GST_CAT_DEFAULT gst_rtmp_output_stream_debug_category
GST_DEBUG_CATEGORY_STATIC (GST_CAT_DEFAULT);

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

struct _GstRtmpOutputStream
{
  GBufferedOutputStream parent_instance;
  GstRtmpChunkStreams *chunk_streams;
  guint chunk_size, ack_window_size;
  guint64 bytes, bytes_acked;
};

typedef struct
{
  GBufferedOutputStreamClass parent_class;
} GstRtmpOutputStreamClass;

G_DEFINE_TYPE (GstRtmpOutputStream, gst_rtmp_output_stream,
    G_TYPE_BUFFERED_OUTPUT_STREAM);

static void gst_rtmp_output_stream_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec);
static void gst_rtmp_output_stream_finalize (GObject * object);

static void
gst_rtmp_output_stream_class_init (GstRtmpOutputStreamClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_class->get_property = gst_rtmp_output_stream_get_property;
  gobject_class->finalize = gst_rtmp_output_stream_finalize;

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

  GST_DEBUG_CATEGORY_INIT (GST_CAT_DEFAULT, "rtmpoutputstream", 0,
      "debug category for GstRtmpOutputStream");
}

GstRtmpOutputStream *
gst_rtmp_output_stream_new (GOutputStream * base_stream)
{
  GstRtmpOutputStream *stream;

  g_return_val_if_fail (G_IS_OUTPUT_STREAM (base_stream), NULL);

  stream = g_object_new (GST_TYPE_RTMP_OUTPUT_STREAM,
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
gst_rtmp_output_stream_init (GstRtmpOutputStream * self)
{
  self->chunk_size = GST_RTMP_DEFAULT_CHUNK_SIZE;
  self->chunk_streams = gst_rtmp_chunk_streams_new ();

  g_object_bind_property_full (self, "chunk-size", self, "buffer-size",
      G_BINDING_SYNC_CREATE, calc_buffer_size, NULL, NULL, NULL);
}

static void
gst_rtmp_output_stream_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec)
{
  GstRtmpOutputStream *self = GST_RTMP_OUTPUT_STREAM (object);

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
gst_rtmp_output_stream_finalize (GObject * object)
{
  GstRtmpOutputStream *self = GST_RTMP_OUTPUT_STREAM (object);

  g_clear_pointer (&self->chunk_streams, gst_rtmp_chunk_streams_free);

  G_OBJECT_CLASS (gst_rtmp_output_stream_parent_class)->finalize (object);
}

void
gst_rtmp_output_stream_write_async (GstRtmpOutputStream * self,
    guint32 chunk_stream, GstBuffer * buffer, GCancellable * cancellable,
    GAsyncReadyCallback callback, gpointer user_data)
{
  GTask *task = g_task_new (self, cancellable, callback, user_data);
  (void) task;
}

gboolean
gst_rtmp_output_stream_write_finish (GstRtmpOutputStream * self,
    GAsyncResult * result, GError ** error)
{
  g_return_val_if_fail (g_task_is_valid (result, self), FALSE);
  return g_task_propagate_boolean (G_TASK (result), error);
}
