/* GStreamer RTMP Library
 * Copyright (C) 2013 David Schleef <ds@schleef.org>
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

#include <gst/gst.h>
#include <string.h>
#include "rtmpconnection.h"
#include "rtmpchunkstream.h"
#include "rtmpmessage.h"
#include "rtmputils.h"
#include "amf.h"

GST_DEBUG_CATEGORY_STATIC (gst_rtmp_connection_debug_category);
#define GST_CAT_DEFAULT gst_rtmp_connection_debug_category

#define READ_SIZE 8192

typedef void (*GstRtmpConnectionCallback) (GstRtmpConnection * connection);

struct _GstRtmpConnection
{
  GObject object;

  /* should be properties */
  gboolean input_paused;
  gboolean error;

  /* private */
  GThread *thread;
  GSocketConnection *connection;
  GCancellable *cancellable;
  GSocketClient *socket_client;
  GAsyncQueue *output_queue;
  GMainContext *main_context;

  GSource *input_source;
  GByteArray *input_bytes;
  guint input_needed_bytes;
  GstRtmpChunkStreams *input_streams, *output_streams;
  GList *command_callbacks;
  guint transaction_count;

  GstRtmpConnectionMessageFunc input_handler;
  gpointer input_handler_user_data;
  GDestroyNotify input_handler_user_data_destroy;

  GstRtmpConnectionFunc output_handler;
  gpointer output_handler_user_data;
  GDestroyNotify output_handler_user_data_destroy;

  gboolean writing;

  /* RTMP configuration */
  gsize in_chunk_size;
  gsize out_chunk_size;
  gsize window_ack_size;
  gsize total_input_bytes;
  gsize bytes_since_ack;
  gsize peer_bandwidth;
};

struct _GstRtmpConnectionClass
{
  GObjectClass object_class;
};

/* prototypes */

static void gst_rtmp_connection_dispose (GObject * object);
static void gst_rtmp_connection_finalize (GObject * object);
static void gst_rtmp_connection_emit_error (GstRtmpConnection * self);
static gboolean gst_rtmp_connection_input_ready (GInputStream * is,
    gpointer user_data);
static void gst_rtmp_connection_start_write (GstRtmpConnection * self);
static void gst_rtmp_connection_write_bytes_done (GObject * obj,
    GAsyncResult * result, gpointer user_data);
static void gst_rtmp_connection_start_read (GstRtmpConnection * sc,
    guint needed_bytes);
static void gst_rtmp_connection_try_read (GstRtmpConnection * sc);
static void gst_rtmp_connection_do_read (GstRtmpConnection * sc);
static void gst_rtmp_connection_handle_protocol_control (GstRtmpConnection *
    connection, GstBuffer * buffer);
static void gst_rtmp_connection_handle_cm (GstRtmpConnection * connection,
    GstBuffer * buffer);
static void gst_rtmp_connection_handle_user_control (GstRtmpConnection * sc,
    GstBuffer * buffer);
static void gst_rtmp_connection_handle_message (GstRtmpConnection * sc,
    GstBuffer * buffer);

static void gst_rtmp_connection_send_ack (GstRtmpConnection * connection);
static void
gst_rtmp_connection_send_ping_response (GstRtmpConnection * connection,
    guint32 event_data);
static void gst_rtmp_connection_send_window_size_request (GstRtmpConnection *
    connection);

typedef struct _CommandCallback CommandCallback;
struct _CommandCallback
{
  guint32 stream_id;
  gdouble transaction_id;
  gchar *command_name;
  GstRtmpCommandCallback func;
  gpointer user_data;
};

static CommandCallback *
command_callback_new (guint32 stream_id, gdouble transaction_id,
    const gchar * command_name, GstRtmpCommandCallback func, gpointer user_data)
{
  CommandCallback *data = g_slice_new (CommandCallback);
  data->stream_id = stream_id;
  data->transaction_id = transaction_id;
  data->command_name = g_strdup (command_name);
  data->func = func;
  data->user_data = user_data;
  return data;
}

static void
command_callback_free (gpointer ptr)
{
  CommandCallback *data = ptr;
  g_free (data->command_name);
  g_slice_free (CommandCallback, data);
}

enum
{
  SIGNAL_ERROR,

  N_SIGNALS
};

static guint signals[N_SIGNALS] = { 0, };

/* class initialization */

G_DEFINE_TYPE_WITH_CODE (GstRtmpConnection, gst_rtmp_connection,
    G_TYPE_OBJECT,
    GST_DEBUG_CATEGORY_INIT (gst_rtmp_connection_debug_category,
        "rtmpconnection", 0, "debug category for GstRtmpConnection class"));

static void
gst_rtmp_connection_class_init (GstRtmpConnectionClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_class->dispose = gst_rtmp_connection_dispose;
  gobject_class->finalize = gst_rtmp_connection_finalize;

  signals[SIGNAL_ERROR] = g_signal_new ("error", G_TYPE_FROM_CLASS (klass),
      G_SIGNAL_RUN_LAST, 0, NULL, NULL, NULL, G_TYPE_NONE, 0);

  GST_DEBUG_REGISTER_FUNCPTR (gst_rtmp_connection_do_read);
}

static void
gst_rtmp_connection_init (GstRtmpConnection * rtmpconnection)
{
  rtmpconnection->cancellable = g_cancellable_new ();
  rtmpconnection->output_queue =
      g_async_queue_new_full ((GDestroyNotify) g_bytes_unref);
  rtmpconnection->input_streams = gst_rtmp_chunk_streams_new ();
  rtmpconnection->output_streams = gst_rtmp_chunk_streams_new ();

  rtmpconnection->in_chunk_size = GST_RTMP_DEFAULT_CHUNK_SIZE;
  rtmpconnection->out_chunk_size = GST_RTMP_DEFAULT_CHUNK_SIZE;

  rtmpconnection->input_bytes = g_byte_array_sized_new (2 * READ_SIZE);
  rtmpconnection->input_needed_bytes = 1;
}

void
gst_rtmp_connection_dispose (GObject * object)
{
  GstRtmpConnection *rtmpconnection = GST_RTMP_CONNECTION (object);
  GST_DEBUG_OBJECT (rtmpconnection, "dispose");

  /* clean up as possible.  may be called multiple times */

  gst_rtmp_connection_close (rtmpconnection);
  g_cancellable_cancel (rtmpconnection->cancellable);
  gst_rtmp_connection_set_input_handler (rtmpconnection, NULL, NULL, NULL);
  gst_rtmp_connection_set_output_handler (rtmpconnection, NULL, NULL, NULL);

  G_OBJECT_CLASS (gst_rtmp_connection_parent_class)->dispose (object);
}

void
gst_rtmp_connection_finalize (GObject * object)
{
  GstRtmpConnection *rtmpconnection = GST_RTMP_CONNECTION (object);
  GST_DEBUG_OBJECT (rtmpconnection, "finalize");

  /* clean up object here */

  g_clear_object (&rtmpconnection->cancellable);
  g_clear_object (&rtmpconnection->connection);
  g_clear_pointer (&rtmpconnection->output_queue, g_async_queue_unref);
  g_clear_pointer (&rtmpconnection->input_streams, gst_rtmp_chunk_streams_free);
  g_clear_pointer (&rtmpconnection->output_streams,
      gst_rtmp_chunk_streams_free);
  g_clear_pointer (&rtmpconnection->input_bytes, g_byte_array_unref);
  g_clear_pointer (&rtmpconnection->main_context, g_main_context_unref);
  g_clear_pointer (&rtmpconnection->thread, g_thread_unref);

  G_OBJECT_CLASS (gst_rtmp_connection_parent_class)->finalize (object);
}

static void
gst_rtmp_connection_set_socket_connection (GstRtmpConnection * sc,
    GSocketConnection * connection)
{
  GInputStream *is;

  sc->thread = g_thread_ref (g_thread_self ());
  sc->main_context = g_main_context_ref_thread_default ();
  sc->connection = g_object_ref (connection);

  /* refs the socket because it's creating an input stream, which holds a ref */
  is = g_io_stream_get_input_stream (G_IO_STREAM (sc->connection));
  /* refs the socket because it's creating a socket source */
  g_warn_if_fail (!sc->input_source);
  sc->input_source =
      g_pollable_input_stream_create_source (G_POLLABLE_INPUT_STREAM (is),
      sc->cancellable);
  g_source_set_callback (sc->input_source,
      (GSourceFunc) gst_rtmp_connection_input_ready, g_object_ref (sc),
      g_object_unref);
  g_source_attach (sc->input_source, sc->main_context);
}

GstRtmpConnection *
gst_rtmp_connection_new (GSocketConnection * connection)
{
  GstRtmpConnection *sc;

  sc = g_object_new (GST_TYPE_RTMP_CONNECTION, NULL);

  gst_rtmp_connection_set_socket_connection (sc, connection);

  return sc;
}

static void
cancel_all_commands (GstRtmpConnection * self)
{
  GList *l;
  for (l = self->command_callbacks; l; l = g_list_next (l)) {
    CommandCallback *cc = l->data;
    GST_TRACE ("calling command callback %s",
        GST_DEBUG_FUNCPTR_NAME (cc->func));
    cc->func ("<cancelled>", NULL, cc->user_data);
  }

  g_list_free_full (self->command_callbacks, command_callback_free);
  self->command_callbacks = NULL;
}

void
gst_rtmp_connection_close (GstRtmpConnection * self)
{
  if (self->thread != g_thread_self ()) {
    GST_ERROR ("Called from wrong thread");
  }

  g_cancellable_cancel (self->cancellable);
  cancel_all_commands (self);

  if (self->input_source) {
    g_source_destroy (self->input_source);
    g_clear_pointer (&self->input_source, g_source_unref);
  }

  if (self->connection) {
    g_io_stream_close_async (G_IO_STREAM (self->connection),
        G_PRIORITY_DEFAULT, NULL, NULL, NULL);
  }
}

void
gst_rtmp_connection_close_and_unref (gpointer ptr)
{
  GstRtmpConnection *connection;

  g_return_if_fail (ptr);

  connection = GST_RTMP_CONNECTION (ptr);
  gst_rtmp_connection_close (connection);
  g_object_unref (connection);
}

void
gst_rtmp_connection_set_input_handler (GstRtmpConnection * sc,
    GstRtmpConnectionMessageFunc callback, gpointer user_data,
    GDestroyNotify user_data_destroy)
{
  if (sc->input_handler_user_data_destroy) {
    sc->input_handler_user_data_destroy (sc->input_handler_user_data);
  }

  sc->input_handler = callback;
  sc->input_handler_user_data = user_data;
  sc->input_handler_user_data_destroy = user_data_destroy;
}

void
gst_rtmp_connection_set_output_handler (GstRtmpConnection * sc,
    GstRtmpConnectionFunc callback, gpointer user_data,
    GDestroyNotify user_data_destroy)
{
  if (sc->output_handler_user_data_destroy) {
    sc->output_handler_user_data_destroy (sc->output_handler_user_data);
  }

  sc->output_handler = callback;
  sc->output_handler_user_data = user_data;
  sc->output_handler_user_data_destroy = user_data_destroy;
}

static gboolean
gst_rtmp_connection_input_ready (GInputStream * is, gpointer user_data)
{
  GstRtmpConnection *sc = user_data;
  gssize ret;
  guint oldsize;
  GError *error = NULL;

  GST_LOG ("input ready");

  oldsize = sc->input_bytes->len;
  g_byte_array_set_size (sc->input_bytes, oldsize + READ_SIZE);
  ret =
      g_pollable_input_stream_read_nonblocking (G_POLLABLE_INPUT_STREAM (is),
      sc->input_bytes->data + oldsize, READ_SIZE, sc->cancellable, &error);
  g_byte_array_set_size (sc->input_bytes, oldsize + (ret > 0 ? ret : 0));

  if (ret < 0) {
    gint code = error->code;

    if (error->domain == G_IO_ERROR && (code == G_IO_ERROR_WOULD_BLOCK ||
            code == G_IO_ERROR_TIMED_OUT || code == G_IO_ERROR_AGAIN)) {
      /* should retry */
      GST_DEBUG ("read IO error %d %s, continuing", code, error->message);
      g_error_free (error);
      return G_SOURCE_CONTINUE;
    }

    GST_ERROR ("read error: %s %d %s", g_quark_to_string (error->domain),
        code, error->message);
    g_error_free (error);
  } else if (ret == 0) {
    GST_INFO ("read EOF");
  }

  if (ret <= 0) {
    gst_rtmp_connection_emit_error (sc);
    return G_SOURCE_REMOVE;
  }

  GST_LOG ("read %" G_GSIZE_FORMAT " bytes", ret);

  sc->total_input_bytes += ret;
  sc->bytes_since_ack += ret;
  if (sc->bytes_since_ack >= sc->window_ack_size) {
    gst_rtmp_connection_send_ack (sc);
  }

  gst_rtmp_connection_try_read (sc);
  return G_SOURCE_CONTINUE;
}

static void
gst_rtmp_connection_start_write (GstRtmpConnection * self)
{
  GOutputStream *os;
  GBytes *bytes;

  if (self->writing) {
    return;
  }

  bytes = g_async_queue_try_pop (self->output_queue);
  if (!bytes) {
    return;
  }

  self->writing = TRUE;
  if (self->output_handler) {
    self->output_handler (self, self->output_handler_user_data);
  }

  os = g_io_stream_get_output_stream (G_IO_STREAM (self->connection));
  gst_rtmp_output_stream_write_all_bytes_async (os, bytes,
      G_PRIORITY_DEFAULT, self->cancellable,
      gst_rtmp_connection_write_bytes_done, g_object_ref (self));
  g_bytes_unref (bytes);
}

static void
gst_rtmp_connection_emit_error (GstRtmpConnection * self)
{
  if (self->error) {
    return;
  }

  GST_INFO ("connection error");
  self->error = TRUE;

  cancel_all_commands (self);

  g_signal_emit (self, signals[SIGNAL_ERROR], 0);
}

static void
gst_rtmp_connection_write_bytes_done (GObject * obj,
    GAsyncResult * result, gpointer user_data)
{
  GOutputStream *os = G_OUTPUT_STREAM (obj);
  GstRtmpConnection *self = GST_RTMP_CONNECTION (user_data);
  GError *error = NULL;
  gboolean res;

  self->writing = FALSE;

  res = gst_rtmp_output_stream_write_all_bytes_finish (os, result, &error);
  if (!res) {
    if (g_error_matches (error, G_IO_ERROR, G_IO_ERROR_CANCELLED)) {
      GST_INFO ("write cancelled");
    } else {
      GST_ERROR ("write error: %s", error->message);
    }
    gst_rtmp_connection_emit_error (self);
    g_error_free (error);
    g_object_unref (self);
    return;
  }

  GST_LOG ("write completed");
  gst_rtmp_connection_start_write (self);
  g_object_unref (self);
}

static void
gst_rtmp_connection_start_read (GstRtmpConnection * connection,
    guint needed_bytes)
{
  g_return_if_fail (needed_bytes > 0);
  connection->input_needed_bytes = needed_bytes;
  gst_rtmp_connection_try_read (connection);
}

static void
gst_rtmp_connection_try_read (GstRtmpConnection * connection)
{
  guint need = connection->input_needed_bytes,
      len = connection->input_bytes->len;

  if (len < need) {
    GST_LOG ("got %u < %u bytes, need more", len, need);
    return;
  }

  GST_LOG ("got %u >= %u bytes, proceeding", len, need);
  gst_rtmp_connection_do_read (connection);
}

static void
gst_rtmp_connection_take_input_bytes (GstRtmpConnection * sc, gsize size,
    GBytes ** outbytes)
{
  g_return_if_fail (size <= sc->input_bytes->len);

  if (outbytes) {
    *outbytes = g_bytes_new (sc->input_bytes->data, size);
  }

  g_byte_array_remove_range (sc->input_bytes, 0, size);
}

static void
gst_rtmp_connection_do_read (GstRtmpConnection * sc)
{
  GByteArray *input_bytes = sc->input_bytes;
  gsize needed_bytes = 1;

  while (1) {
    GstRtmpChunkStream *cstream;
    guint32 chunk_stream_id, header_size, next_size;
    guint8 *data;

    chunk_stream_id = gst_rtmp_chunk_stream_parse_id (input_bytes->data,
        input_bytes->len);

    if (!chunk_stream_id) {
      needed_bytes = input_bytes->len + 1;
      break;
    }

    cstream = gst_rtmp_chunk_streams_get (sc->input_streams, chunk_stream_id);
    header_size = gst_rtmp_chunk_stream_parse_header (cstream,
        input_bytes->data, input_bytes->len);

    if (input_bytes->len < header_size) {
      needed_bytes = header_size;
      break;
    }

    next_size = gst_rtmp_chunk_stream_parse_payload (cstream,
        sc->in_chunk_size, &data);

    if (input_bytes->len < header_size + next_size) {
      needed_bytes = header_size + next_size;
      break;
    }

    memcpy (data, input_bytes->data + header_size, next_size);
    gst_rtmp_connection_take_input_bytes (sc, header_size + next_size, NULL);

    next_size = gst_rtmp_chunk_stream_wrote_payload (cstream,
        sc->in_chunk_size);

    if (next_size == 0) {
      GstBuffer *buffer = gst_rtmp_chunk_stream_parse_finish (cstream);
      gst_rtmp_connection_handle_message (sc, buffer);
      gst_buffer_unref (buffer);
    }
  }

  gst_rtmp_connection_start_read (sc, needed_bytes);
}

static void
gst_rtmp_connection_handle_message (GstRtmpConnection * sc, GstBuffer * buffer)
{
  if (gst_rtmp_message_is_protocol_control (buffer)) {
    gst_rtmp_connection_handle_protocol_control (sc, buffer);
    return;
  }

  if (gst_rtmp_message_is_user_control (buffer)) {
    gst_rtmp_connection_handle_user_control (sc, buffer);
    return;
  }

  switch (gst_rtmp_message_get_type (buffer)) {
    case GST_RTMP_MESSAGE_TYPE_COMMAND_AMF0:
      gst_rtmp_connection_handle_cm (sc, buffer);
      return;

    default:
      if (sc->input_handler) {
        sc->input_handler (sc, buffer, sc->input_handler_user_data);
      }
      return;
  }
}

static void
gst_rtmp_connection_handle_protocol_control (GstRtmpConnection * connection,
    GstBuffer * buffer)
{
  GstRtmpMessageType type = gst_rtmp_message_get_type (buffer);
  GstMapInfo map;
  guint32 param, param2;

  if (!gst_buffer_map (buffer, &map, GST_MAP_READ)) {
    GST_ERROR ("can't map protocol control message");
    return;
  }

  GST_LOG ("got protocol control message %d:%s", type,
      gst_rtmp_message_type_get_nick (type));

  switch (type) {
    case GST_RTMP_MESSAGE_TYPE_SET_CHUNK_SIZE:
      if (map.size < 4) {
        GST_ERROR ("can't read chunk size");
        break;
      }
      param = GST_READ_UINT32_BE (map.data);
      GST_INFO ("new chunk size %" G_GUINT32_FORMAT, param);
      connection->in_chunk_size = param;
      break;

    case GST_RTMP_MESSAGE_TYPE_ABORT_MESSAGE:
      if (map.size < 4) {
        GST_ERROR ("can't read stream id");
        break;
      }
      param = GST_READ_UINT32_BE (map.data);
      GST_ERROR ("unimplemented: chunk abort, stream_id = %" G_GUINT32_FORMAT,
          param);
      break;

    case GST_RTMP_MESSAGE_TYPE_ACKNOWLEDGEMENT:
      if (map.size < 4) {
        GST_ERROR ("can't read acked bytes");
        break;
      }
      param = GST_READ_UINT32_BE (map.data);
      /* We don't really send ack requests that we care about, so ignore */
      GST_DEBUG ("acknowledgement %" G_GUINT32_FORMAT, param);
      break;

    case GST_RTMP_MESSAGE_TYPE_WINDOW_ACK_SIZE:
      if (map.size < 4) {
        GST_ERROR ("can't read window ack size");
        break;
      }
      param = GST_READ_UINT32_BE (map.data);
      GST_INFO ("window ack size: %" G_GUINT32_FORMAT, param);
      connection->window_ack_size = param;
      break;

    case GST_RTMP_MESSAGE_TYPE_SET_PEER_BANDWIDTH:
      if (map.size < 5) {
        GST_ERROR ("can't read peer bandwidth");
        break;
      }
      param = GST_READ_UINT32_BE (map.data);
      param2 = GST_READ_UINT8 (map.data + 4);
      GST_FIXME ("set peer bandwidth: %" G_GUINT32_FORMAT ", %"
          G_GUINT32_FORMAT, param, param2);
      /* FIXME this is not correct, but close enough */
      if (connection->peer_bandwidth != param) {
        connection->peer_bandwidth = param;
        gst_rtmp_connection_send_window_size_request (connection);
      }
      break;

    default:
      GST_ERROR ("unimplemented protocol control type %d:%s", type,
          gst_rtmp_message_type_get_nick (type));
      break;
  }

  gst_buffer_unmap (buffer, &map);
}

static void
gst_rtmp_connection_handle_user_control (GstRtmpConnection * connection,
    GstBuffer * buffer)
{
  GstRtmpUserControlType type;
  GstMapInfo map;
  guint32 param, param2;

  if (!gst_buffer_map (buffer, &map, GST_MAP_READ)) {
    GST_ERROR ("can't map user control message");
    return;
  }

  if (map.size < 2) {
    GST_ERROR ("can't read user control type");
    gst_buffer_unmap (buffer, &map);
    return;
  }

  type = GST_READ_UINT16_BE (map.data);
  GST_LOG ("got user control message %d:%s", type,
      gst_rtmp_user_control_type_get_nick (type));

  switch (type) {
    case GST_RTMP_USER_CONTROL_TYPE_STREAM_BEGIN:
      if (map.size < 6) {
        GST_ERROR ("can't read stream id");
        break;
      }
      param = GST_READ_UINT32_BE (map.data + 2);
      GST_DEBUG ("ignoring stream begin: %" G_GUINT32_FORMAT, param);
      break;

    case GST_RTMP_USER_CONTROL_TYPE_STREAM_EOF:
      if (map.size < 6) {
        GST_ERROR ("can't read stream id");
        break;
      }
      param = GST_READ_UINT32_BE (map.data + 2);
      GST_ERROR ("unimplemented stream EOF: %" G_GUINT32_FORMAT, param);
      break;

    case GST_RTMP_USER_CONTROL_TYPE_STREAM_DRY:
      if (map.size < 6) {
        GST_ERROR ("can't read stream id");
        break;
      }
      param = GST_READ_UINT32_BE (map.data + 2);
      GST_DEBUG ("ignoring stream dry: %" G_GUINT32_FORMAT, param);
      break;

    case GST_RTMP_USER_CONTROL_TYPE_SET_BUFFER_LENGTH:
      if (map.size < 10) {
        GST_ERROR ("can't read buffer length");
        break;
      }
      param = GST_READ_UINT32_BE (map.data + 2);
      param2 = GST_READ_UINT32_BE (map.data + 6);
      GST_FIXME ("ignoring set buffer length: %" G_GUINT32_FORMAT ", %"
          G_GUINT32_FORMAT " ms", param, param2);
      break;

    case GST_RTMP_USER_CONTROL_TYPE_STREAM_IS_RECORDED:
      if (map.size < 6) {
        GST_ERROR ("can't read stream id");
        break;
      }
      param = GST_READ_UINT32_BE (map.data + 2);
      GST_ERROR ("unimplemented stream-is-recorded: %" G_GUINT32_FORMAT, param);
      break;

    case GST_RTMP_USER_CONTROL_TYPE_PING_REQUEST:
      if (map.size < 6) {
        GST_ERROR ("can't read ping data");
        break;
      }
      param = GST_READ_UINT32_BE (map.data + 2);
      GST_DEBUG ("ping request: %" G_GUINT32_FORMAT, param);
      gst_rtmp_connection_send_ping_response (connection, param);
      break;

    case GST_RTMP_USER_CONTROL_TYPE_PING_RESPONSE:
      if (map.size < 6) {
        GST_ERROR ("can't read ping data");
        break;
      }
      param = GST_READ_UINT32_BE (map.data + 2);
      GST_DEBUG ("ignoring ping response: %" G_GUINT32_FORMAT, param);
      break;

    default:
      GST_ERROR ("unimplemented user control type %d:%s", type,
          gst_rtmp_user_control_type_get_nick (type));
      break;
  }

  gst_buffer_unmap (buffer, &map);
}

static void
gst_rtmp_connection_handle_cm (GstRtmpConnection * sc, GstBuffer * buffer)
{
  GstRtmpMeta *meta = gst_buffer_get_rtmp_meta (buffer);
  gchar *command_name;
  gdouble transaction_id;
  GPtrArray *args;
  GList *l;
  GstMapInfo map;

  gst_buffer_map (buffer, &map, GST_MAP_READ);

  args = gst_amf_parse_command (map.data, map.size, &transaction_id,
      &command_name);

  gst_buffer_unmap (buffer, &map);

  if (!args) {
    return;
  }

  if (transaction_id < 0 || transaction_id > G_MAXUINT) {
    GST_WARNING ("Server sent extreme transaction id %.0f", transaction_id);
  } else if (transaction_id > sc->transaction_count) {
    GST_WARNING ("Server sent command with unused transaction ID (%.0f > %u)",
        transaction_id, sc->transaction_count);
    sc->transaction_count = transaction_id;
  }

  GST_DEBUG ("got control message \"%s\" transaction %.0f size %"
      G_GUINT32_FORMAT, GST_STR_NULL (command_name), transaction_id,
      meta->size);

  for (l = sc->command_callbacks; l; l = g_list_next (l)) {
    CommandCallback *cc = l->data;

    if (cc->stream_id != meta->mstream) {
      continue;
    }

    if (cc->transaction_id != transaction_id) {
      continue;
    }

    if (cc->command_name && g_strcmp0 (cc->command_name, command_name)) {
      continue;
    }

    GST_TRACE ("calling command callback %s",
        GST_DEBUG_FUNCPTR_NAME (cc->func));
    sc->command_callbacks = g_list_remove_link (sc->command_callbacks, l);
    cc->func (command_name, args, cc->user_data);
    g_list_free_full (l, command_callback_free);
    break;
  }

  g_free (command_name);
  g_ptr_array_unref (args);
}

static gboolean
start_write (gpointer user_data)
{
  GstRtmpConnection *sc = user_data;
  gst_rtmp_connection_start_write (sc);
  g_object_unref (sc);
  return G_SOURCE_REMOVE;
}

static void
byte_array_take_buffer (GByteArray * byte_array, GstBuffer * buffer)
{
  GstMapInfo map;
  gboolean ret;
  ret = gst_buffer_map (buffer, &map, GST_MAP_READ);
  g_assert (ret);
  g_assert (byte_array->len + map.size <= (guint64) G_MAXUINT);
  g_byte_array_append (byte_array, map.data, map.size);
  gst_buffer_unmap (buffer, &map);
  gst_buffer_unref (buffer);
}

void
gst_rtmp_connection_queue_message (GstRtmpConnection * self, GstBuffer * buffer)
{
  GstRtmpMeta *meta;
  GstRtmpChunkStream *cstream;
  GstBuffer *out_buffer;
  GByteArray *out_ba;

  g_return_if_fail (GST_IS_RTMP_CONNECTION (self));
  g_return_if_fail (GST_IS_BUFFER (buffer));

  meta = gst_buffer_get_rtmp_meta (buffer);
  g_return_if_fail (meta);

  cstream = gst_rtmp_chunk_streams_get (self->output_streams, meta->cstream);
  g_return_if_fail (cstream);

  out_buffer = gst_rtmp_chunk_stream_serialize_start (cstream, buffer,
      self->out_chunk_size);
  g_return_if_fail (out_buffer);

  out_ba = g_byte_array_new ();

  while (out_buffer) {
    byte_array_take_buffer (out_ba, out_buffer);

    out_buffer = gst_rtmp_chunk_stream_serialize_next (cstream,
        self->out_chunk_size);
  }

  g_async_queue_push (self->output_queue, g_byte_array_free_to_bytes (out_ba));
  g_main_context_invoke (self->main_context, start_write, g_object_ref (self));
}

guint
gst_rtmp_connection_get_num_queued (GstRtmpConnection * connection)
{
  return g_async_queue_length (connection->output_queue);
}

guint
gst_rtmp_connection_send_command (GstRtmpConnection * connection,
    GstRtmpCommandCallback response_command, gpointer user_data,
    guint32 stream_id, const gchar * command_name, const GstAmfNode * argument,
    ...)
{
  GstBuffer *buffer;
  gdouble transaction_id = 0;
  va_list ap;
  GBytes *payload;
  guint8 *data;
  gsize size;

  if (connection->thread != g_thread_self ()) {
    GST_ERROR ("Called from wrong thread");
  }

  if (response_command) {
    CommandCallback *cc;

    transaction_id = ++connection->transaction_count;

    GST_TRACE ("Registering %s for transid %.0f",
        GST_DEBUG_FUNCPTR_NAME (response_command), transaction_id);

    cc = command_callback_new (stream_id, transaction_id, NULL,
        response_command, user_data);

    connection->command_callbacks =
        g_list_append (connection->command_callbacks, cc);
  }

  va_start (ap, argument);
  payload = gst_amf_serialize_command_valist (transaction_id,
      command_name, argument, ap);
  va_end (ap);

  data = g_bytes_unref_to_data (payload, &size);
  buffer = gst_rtmp_message_new_wrapped (GST_RTMP_MESSAGE_TYPE_COMMAND_AMF0,
      3, stream_id, data, size);

  gst_rtmp_connection_queue_message (connection, buffer);
  return transaction_id;
}

void
gst_rtmp_connection_expect_command (GstRtmpConnection * connection,
    GstRtmpCommandCallback response_command, gpointer user_data,
    guint32 stream_id, const gchar * command_name)
{
  CommandCallback *cc;

  g_return_if_fail (response_command);
  g_return_if_fail (command_name);

  GST_TRACE ("Registering %s for stream id %" G_GUINT32_FORMAT
      " name \"%s\"", GST_DEBUG_FUNCPTR_NAME (response_command),
      stream_id, command_name);

  cc = command_callback_new (stream_id, 0, command_name, response_command,
      user_data);

  connection->command_callbacks =
      g_list_append (connection->command_callbacks, cc);
}

static void
gst_rtmp_connection_send_ack (GstRtmpConnection * connection)
{
  GstBuffer *buffer;
  guint8 *data;

  data = g_malloc (4);
  GST_WRITE_UINT32_BE (data, connection->total_input_bytes);

  buffer = gst_rtmp_message_new_wrapped (GST_RTMP_MESSAGE_TYPE_ACKNOWLEDGEMENT,
      2, 0, data, 4);

  gst_rtmp_connection_queue_message (connection, buffer);

  connection->bytes_since_ack = 0;
}

static void
gst_rtmp_connection_send_ping_response (GstRtmpConnection * connection,
    guint32 event_data)
{
  GstBuffer *buffer;
  guint8 *data;

  data = g_malloc (6);
  GST_WRITE_UINT16_BE (data, GST_RTMP_USER_CONTROL_TYPE_PING_RESPONSE);
  GST_WRITE_UINT32_BE (data + 2, event_data);

  buffer = gst_rtmp_message_new_wrapped (GST_RTMP_MESSAGE_TYPE_USER_CONTROL,
      2, 0, data, 6);

  gst_rtmp_connection_queue_message (connection, buffer);
}

static void
gst_rtmp_connection_send_window_size_request (GstRtmpConnection * connection)
{
  GstBuffer *buffer;
  guint8 *data;

  data = g_malloc (4);
  GST_WRITE_UINT32_BE (data, connection->peer_bandwidth);

  buffer = gst_rtmp_message_new_wrapped (GST_RTMP_MESSAGE_TYPE_WINDOW_ACK_SIZE,
      2, 0, data, 4);

  gst_rtmp_connection_queue_message (connection, buffer);
}
