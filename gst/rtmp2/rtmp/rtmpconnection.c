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
#include "rtmpconnection.h"
#include "rtmpchunk.h"
#include "amf.h"
#include "rtmputils.h"

#include <string.h>

GST_DEBUG_CATEGORY_STATIC (gst_rtmp_connection_debug_category);
#define GST_CAT_DEFAULT gst_rtmp_connection_debug_category

/* prototypes */

static void gst_rtmp_connection_dispose (GObject * object);
static void gst_rtmp_connection_finalize (GObject * object);
static void gst_rtmp_connection_got_closed (GstRtmpConnection * connection);
static gboolean gst_rtmp_connection_input_ready (GInputStream * is,
    gpointer user_data);
static gboolean gst_rtmp_connection_output_ready (GOutputStream * os,
    gpointer user_data);
static void gst_rtmp_connection_client_handshake1 (GstRtmpConnection * sc);
static void gst_rtmp_connection_client_handshake1_done (GObject * obj,
    GAsyncResult * res, gpointer user_data);
static void gst_rtmp_connection_client_handshake2 (GstRtmpConnection * sc);
static void gst_rtmp_connection_client_handshake2_done (GObject * obj,
    GAsyncResult * res, gpointer user_data);
static void gst_rtmp_connection_server_handshake1 (GstRtmpConnection * sc);
static void gst_rtmp_connection_server_handshake1_done (GObject * obj,
    GAsyncResult * res, gpointer user_data);
static void gst_rtmp_connection_server_handshake2 (GstRtmpConnection * sc);
static void gst_rtmp_connection_write_chunk_done (GObject * obj,
    GAsyncResult * res, gpointer user_data);
static void
gst_rtmp_connection_set_input_callback (GstRtmpConnection * connection,
    void (*input_callback) (GstRtmpConnection * connection),
    gsize needed_bytes);
static void gst_rtmp_connection_chunk_callback (GstRtmpConnection * sc);
static void gst_rtmp_connection_start_output (GstRtmpConnection * sc);
static void
gst_rtmp_connection_handle_pcm (GstRtmpConnection * connection,
    GstRtmpChunk * chunk);
static void
gst_rtmp_connection_handle_user_control (GstRtmpConnection * connectin,
    guint32 event_type, guint32 event_data);
static void gst_rtmp_connection_handle_chunk (GstRtmpConnection * sc,
    GstRtmpChunk * chunk);

static void gst_rtmp_connection_send_ack (GstRtmpConnection * connection);
static void
gst_rtmp_connection_send_ping_response (GstRtmpConnection * connection,
    guint32 event_data);
static void gst_rtmp_connection_send_window_size_request (GstRtmpConnection *
    connection);

typedef struct _CommandCallback CommandCallback;
struct _CommandCallback
{
  guint32 chunk_stream_id;
  int transaction_id;
  GstRtmpCommandCallback func;
  gpointer user_data;
};

enum
{
  SIGNAL_CLOSED,

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

  signals[SIGNAL_CLOSED] = g_signal_new ("closed", G_TYPE_FROM_CLASS (klass),
      G_SIGNAL_RUN_LAST, 0, NULL, NULL, NULL, G_TYPE_NONE, 0);
}

static void
gst_rtmp_connection_init (GstRtmpConnection * rtmpconnection)
{
  rtmpconnection->cancellable = g_cancellable_new ();
  rtmpconnection->output_queue = g_async_queue_new_full (gst_rtmp_chunk_free);
  rtmpconnection->input_chunk_cache = gst_rtmp_chunk_cache_new ();

  rtmpconnection->in_chunk_size = 128;
  rtmpconnection->out_chunk_size = 128;

  rtmpconnection->input_bytes = g_byte_array_sized_new (8192);
}

void
gst_rtmp_connection_dispose (GObject * object)
{
  GstRtmpConnection *rtmpconnection = GST_RTMP_CONNECTION (object);

  GST_DEBUG_OBJECT (rtmpconnection, "dispose");

  /* clean up as possible.  may be called multiple times */

  gst_rtmp_connection_close (rtmpconnection);
  g_cancellable_cancel (rtmpconnection->cancellable);
  gst_rtmp_connection_set_chunk_callback (rtmpconnection, NULL, NULL, NULL);

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
  g_clear_pointer (&rtmpconnection->input_chunk_cache,
      gst_rtmp_chunk_cache_free);
  g_clear_pointer (&rtmpconnection->output_bytes, g_bytes_unref);
  g_clear_pointer (&rtmpconnection->output_chunk, gst_rtmp_chunk_free);

  g_clear_pointer (&rtmpconnection->input_bytes, g_byte_array_unref);

  G_OBJECT_CLASS (gst_rtmp_connection_parent_class)->finalize (object);
}

static void
gst_rtmp_connection_set_socket_connection (GstRtmpConnection * sc,
    GSocketConnection * connection)
{
  GInputStream *is;

  sc->thread = g_thread_self ();
  sc->main_context = g_main_context_ref_thread_default ();
  sc->connection = connection;

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

void
gst_rtmp_connection_close (GstRtmpConnection * connection)
{
  if (connection->thread != g_thread_self ()) {
    GST_ERROR ("Called from wrong thread");
  }

  g_cancellable_cancel (connection->cancellable);

  if (connection->input_source) {
    g_source_destroy (connection->input_source);
    g_clear_pointer (&connection->input_source, g_source_unref);
  }
  if (connection->output_source) {
    g_source_destroy (connection->output_source);
    g_clear_pointer (&connection->output_source, g_source_unref);
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
gst_rtmp_connection_set_chunk_callback (GstRtmpConnection * sc,
    GstRtmpConnectionGotChunkFunc callback, gpointer user_data,
    GDestroyNotify user_data_destroy)
{
  if (sc->chunk_handler_callback_user_data_destroy) {
    sc->chunk_handler_callback_user_data_destroy
        (sc->chunk_handler_callback_user_data);
  }

  sc->chunk_handler_callback = callback;
  sc->chunk_handler_callback_user_data = user_data;
  sc->chunk_handler_callback_user_data_destroy = user_data_destroy;
}

static gboolean
start_output (gpointer user_priv)
{
  GstRtmpConnection *sc = GST_RTMP_CONNECTION (user_priv);
  GOutputStream *os;

  if (!sc->handshake_complete)
    return G_SOURCE_REMOVE;

  if (sc->output_source)
    return G_SOURCE_REMOVE;

  os = g_io_stream_get_output_stream (G_IO_STREAM (sc->connection));
  g_warn_if_fail (!sc->output_source);
  sc->output_source =
      g_pollable_output_stream_create_source (G_POLLABLE_OUTPUT_STREAM (os),
      sc->cancellable);
  g_source_set_callback (sc->output_source,
      (GSourceFunc) gst_rtmp_connection_output_ready, g_object_ref (sc),
      g_object_unref);
  g_source_attach (sc->output_source, sc->main_context);

  return G_SOURCE_REMOVE;
}

static void
gst_rtmp_connection_start_output (GstRtmpConnection * sc)
{
  GSource *source;

  source = g_idle_source_new ();
  g_source_set_callback (source, start_output, g_object_ref (sc),
      g_object_unref);
  g_source_attach (source, sc->main_context);
}

static gboolean
gst_rtmp_connection_input_ready (GInputStream * is, gpointer user_data)
{
  GstRtmpConnection *sc;
  gssize ret;
  guint oldsize;
  GError *error = NULL;

  GST_LOG ("input ready");

  if (g_source_is_destroyed (g_main_current_source ())) {
    GST_DEBUG ("spurious input_ready callback");
    return G_SOURCE_REMOVE;
  }

  sc = GST_RTMP_CONNECTION (user_data);
  if (sc->thread != g_thread_self ()) {
    GST_ERROR ("input_ready: Called from wrong thread");
  }

  oldsize = sc->input_bytes->len;
  g_byte_array_set_size (sc->input_bytes, oldsize + 4096);
  ret =
      g_pollable_input_stream_read_nonblocking (G_POLLABLE_INPUT_STREAM (is),
      sc->input_bytes->data + oldsize, 4096, sc->cancellable, &error);
  g_byte_array_set_size (sc->input_bytes, oldsize + (ret > 0 ? ret : 0));

  if (ret < 0) {
    if (error->code == G_IO_ERROR_TIMED_OUT) {
      /* should retry */
      GST_DEBUG ("timeout, continuing");
      g_error_free (error);
      return G_SOURCE_CONTINUE;
    } else {
      GST_ERROR ("read error: %s %d %s", g_quark_to_string (error->domain),
          error->code, error->message);
    }
    g_error_free (error);
    return G_SOURCE_REMOVE;
  }
  if (ret == 0) {
    gst_rtmp_connection_got_closed (sc);
    return G_SOURCE_REMOVE;
  }

  GST_LOG ("read %" G_GSIZE_FORMAT " bytes", ret);

  sc->total_input_bytes += ret;
  sc->bytes_since_ack += ret;
  if (sc->bytes_since_ack >= sc->window_ack_size) {
    gst_rtmp_connection_send_ack (sc);
  }

  GST_LOG ("needed: %" G_GSIZE_FORMAT, sc->input_needed_bytes);

  while (sc->input_callback && sc->input_bytes->len >= sc->input_needed_bytes) {
    GstRtmpConnectionCallback callback;
    GST_DEBUG ("got %u bytes, calling callback", sc->input_bytes->len);
    callback = sc->input_callback;
    sc->input_callback = NULL;
    (*callback) (sc);
  }

  return G_SOURCE_CONTINUE;
}

static gboolean
gst_rtmp_connection_output_ready (GOutputStream * os, gpointer user_data)
{
  GstRtmpConnection *sc;
  GstRtmpChunk *chunk;
  const guint8 *data;
  gsize size;

  GST_LOG ("output ready");

  if (g_source_is_destroyed (g_main_current_source ())) {
    GST_DEBUG ("spurious output_ready callback");
    return G_SOURCE_REMOVE;
  }

  sc = GST_RTMP_CONNECTION (user_data);
  if (sc->thread != g_thread_self ()) {
    GST_ERROR ("input_ready: Called from wrong thread");
  }

  sc->output_source = NULL;

  if (sc->writing)
    return G_SOURCE_REMOVE;

  if (sc->output_chunk) {
    chunk = sc->output_chunk;
  } else {
    chunk = g_async_queue_try_pop (sc->output_queue);
    if (!chunk) {
      return G_SOURCE_REMOVE;
    }
    sc->output_chunk = chunk;

    sc->output_bytes =
        gst_rtmp_chunk_serialize (chunk, NULL, sc->out_chunk_size);
  }

  os = g_io_stream_get_output_stream (G_IO_STREAM (sc->connection));
  data = g_bytes_get_data (sc->output_bytes, &size);
  g_output_stream_write_async (os, data, size, G_PRIORITY_DEFAULT,
      sc->cancellable, gst_rtmp_connection_write_chunk_done, g_object_ref (sc));
  sc->writing = TRUE;

  return G_SOURCE_REMOVE;
}

static void
gst_rtmp_connection_got_closed (GstRtmpConnection * connection)
{
  connection->closed = TRUE;
  g_signal_emit (connection, signals[SIGNAL_CLOSED], 0);
}

static void
gst_rtmp_connection_write_chunk_done (GObject * obj,
    GAsyncResult * res, gpointer user_data)
{
  GOutputStream *os = G_OUTPUT_STREAM (obj);
  GstRtmpConnection *connection = GST_RTMP_CONNECTION (user_data);
  GError *error = NULL;
  gssize ret, expected_size;

  GST_LOG ("gst_rtmp_connection_write_chunk_done");

  expected_size = (gssize) g_bytes_get_size (connection->output_bytes);
  connection->writing = FALSE;

  ret = g_output_stream_write_finish (os, res, &error);
  if (ret < 0) {
    if (g_error_matches (error, G_IO_ERROR, G_IO_ERROR_CANCELLED)) {
      GST_INFO ("write cancelled");
    } else {
      GST_ERROR ("write error: %s", error->message);
    }
    gst_rtmp_connection_got_closed (connection);
    g_error_free (error);
  } else if (ret < expected_size) {
    GST_DEBUG ("short write %" G_GSIZE_FORMAT " < %" G_GSIZE_FORMAT,
        ret, g_bytes_get_size (connection->output_bytes));

    connection->output_bytes =
        gst_rtmp_bytes_remove (connection->output_bytes, ret);
  }

  if (ret < 0 || ret == expected_size) {
    g_clear_pointer (&connection->output_bytes, g_bytes_unref);
    g_clear_pointer (&connection->output_chunk, gst_rtmp_chunk_free);
  }

  if (ret >= 0) {
    gst_rtmp_connection_start_output (connection);
  }

  g_object_unref (connection);
}


G_GNUC_UNUSED static void
parse_message (guint8 * data, int size)
{
  int offset;
  gsize bytes_read;
  GstAmfNode *node;

  offset = 4;

  node = gst_amf_node_new_parse (data + offset, size - offset, &bytes_read);
  offset += bytes_read;
  g_print ("bytes_read: %" G_GSIZE_FORMAT "\n", bytes_read);
  if (node)
    gst_amf_node_free (node);

  node = gst_amf_node_new_parse (data + offset, size - offset, &bytes_read);
  offset += bytes_read;
  g_print ("bytes_read: %" G_GSIZE_FORMAT "\n", bytes_read);
  if (node)
    gst_amf_node_free (node);

  node = gst_amf_node_new_parse (data + offset, size - offset, &bytes_read);
  offset += bytes_read;
  g_print ("bytes_read: %" G_GSIZE_FORMAT "\n", bytes_read);
  if (node)
    gst_amf_node_free (node);

}

static void
gst_rtmp_connection_take_input_bytes (GstRtmpConnection * sc, gsize size,
    GBytes ** outbytes)
{
  gsize current_size;

  current_size = sc->input_bytes->len;
  if (size > current_size) {
    g_critical ("Tried to take too many bytes; %" G_GSIZE_FORMAT
        " > %" G_GSIZE_FORMAT, size, current_size);
    return;
  }

  if (outbytes) {
    *outbytes = g_bytes_new (sc->input_bytes->data, size);
  }

  g_byte_array_remove_range (sc->input_bytes, 0, size);
}

static void
gst_rtmp_connection_server_handshake1 (GstRtmpConnection * sc)
{
  GOutputStream *os;
  GBytes *bytes;
  guint8 *data;

  gst_rtmp_connection_take_input_bytes (sc, 1537, &bytes);
  data = g_malloc (1 + 1536 + 1536);
  memcpy (data, g_bytes_get_data (bytes, NULL), 1 + 1536);
  memset (data + 1537, 0, 8);
  memset (data + 1537 + 8, 0xef, 1528);
  g_bytes_unref (bytes);

  os = g_io_stream_get_output_stream (G_IO_STREAM (sc->connection));
  g_output_stream_write_async (os, data, 1 + 1536 + 1536,
      G_PRIORITY_DEFAULT, sc->cancellable,
      gst_rtmp_connection_server_handshake1_done, sc);
}

static void
gst_rtmp_connection_server_handshake1_done (GObject * obj,
    GAsyncResult * res, gpointer user_data)
{
  GOutputStream *os = G_OUTPUT_STREAM (obj);
  GstRtmpConnection *sc = GST_RTMP_CONNECTION (user_data);
  GError *error = NULL;
  gssize ret;

  GST_DEBUG ("gst_rtmp_connection_server_handshake2_done");

  ret = g_output_stream_write_finish (os, res, &error);
  if (ret < 1 + 1536 + 1536) {
    GST_ERROR ("read error: %s", error->message);
    g_error_free (error);
    return;
  }
  GST_DEBUG ("wrote %" G_GSSIZE_FORMAT " bytes", ret);

  gst_rtmp_connection_set_input_callback (sc,
      gst_rtmp_connection_server_handshake2, 1536);
}

static void
gst_rtmp_connection_server_handshake2 (GstRtmpConnection * sc)
{
  GBytes *bytes;

  gst_rtmp_connection_take_input_bytes (sc, 1536, &bytes);
  g_bytes_unref (bytes);

  /* handshake finished */
  GST_INFO ("server handshake finished");
  sc->handshake_complete = TRUE;

  if (sc->input_bytes->len >= 1) {
    GST_DEBUG ("spare bytes after handshake: %u", sc->input_bytes->len);
    gst_rtmp_connection_chunk_callback (sc);
  }

  gst_rtmp_connection_set_input_callback (sc,
      gst_rtmp_connection_chunk_callback, 0);

  gst_rtmp_connection_start_output (sc);
}

static void
gst_rtmp_connection_chunk_callback (GstRtmpConnection * sc)
{
  gsize needed_bytes = 0;
  gsize size = 0;

  while (1) {
    GstRtmpChunkHeader header = { 0 };
    GstRtmpChunkCacheEntry *entry;
    gboolean ret;
    gsize remaining_bytes;
    gsize chunk_bytes;
    const guint8 *data;

    if (sc->input_bytes->len == 0)
      break;

    ret = gst_rtmp_chunk_parse_header1 (&header, sc->input_bytes);
    if (!ret) {
      needed_bytes = header.header_size;
      break;
    }

    entry =
        gst_rtmp_chunk_cache_get (sc->input_chunk_cache,
        header.chunk_stream_id);

    if (entry->chunk && header.format != 3) {
      GST_ERROR ("expected message continuation, but got new message");
      g_clear_pointer (&entry->chunk, gst_rtmp_chunk_free);
    }

    ret = gst_rtmp_chunk_parse_header2 (&header, sc->input_bytes,
        &entry->previous_header);
    if (!ret) {
      needed_bytes = header.header_size;
      break;
    }

    remaining_bytes = header.message_length - entry->offset;
    chunk_bytes = MIN (remaining_bytes, sc->in_chunk_size);
    data = sc->input_bytes->data;
    size = sc->input_bytes->len;

    if (header.header_size + chunk_bytes > size) {
      needed_bytes = header.header_size + chunk_bytes;
      break;
    }

    if (entry->chunk == NULL) {
      entry->chunk = gst_rtmp_chunk_new ();
      entry->chunk->chunk_stream_id = header.chunk_stream_id;
      entry->chunk->timestamp = header.timestamp;
      entry->chunk->message_length = header.message_length;
      entry->chunk->message_type_id = header.message_type_id;
      entry->chunk->stream_id = header.stream_id;
      entry->chunk->payload = g_bytes_new_take
          (g_malloc (header.message_length), header.message_length);
    }
    memcpy (&entry->previous_header, &header, sizeof (header));
    memcpy ((guint8 *) g_bytes_get_data (entry->chunk->payload,
            NULL) + entry->offset, data + header.header_size, chunk_bytes);
    entry->offset += chunk_bytes;

    gst_rtmp_connection_take_input_bytes (sc,
        header.header_size + chunk_bytes, NULL);

    if (entry->offset == header.message_length) {
      gst_rtmp_connection_handle_chunk (sc, entry->chunk);
      entry->chunk = NULL;
      entry->offset = 0;
    }
  }
  GST_LOG ("setting needed bytes to %" G_GSIZE_FORMAT ", have %"
      G_GSIZE_FORMAT, needed_bytes, size);
  gst_rtmp_connection_set_input_callback (sc,
      gst_rtmp_connection_chunk_callback, needed_bytes);
}

static void
gst_rtmp_connection_handle_chunk (GstRtmpConnection * sc, GstRtmpChunk * chunk)
{
  if (chunk->chunk_stream_id == GST_RTMP_CHUNK_STREAM_PROTOCOL) {
    GST_DEBUG ("got protocol control message, type: %d",
        chunk->message_type_id);
    gst_rtmp_connection_handle_pcm (sc, chunk);
    gst_rtmp_chunk_free (chunk);
  } else {
    if (chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_COMMAND) {
      CommandCallback *cb = NULL;
      GList *g;
      char *command_name;
      double transaction_id;
      GstAmfNode *command_object;
      GstAmfNode *optional_args;

      gst_rtmp_chunk_parse_message (chunk, &command_name, &transaction_id,
          &command_object, &optional_args);
      for (g = sc->command_callbacks; g; g = g_list_next (g)) {
        cb = g->data;
        if (cb->chunk_stream_id == chunk->chunk_stream_id &&
            cb->transaction_id == transaction_id) {
          break;
        }
      }
      if (cb) {
        sc->command_callbacks = g_list_remove (sc->command_callbacks, cb);
        cb->func (sc, chunk, command_name, transaction_id, command_object,
            optional_args, cb->user_data);
        g_free (cb);
      }
      g_free (command_name);
      gst_amf_node_free (command_object);
      if (optional_args)
        gst_amf_node_free (optional_args);
    }
    GST_LOG ("got chunk: %" G_GSIZE_FORMAT " bytes", chunk->message_length);
    if (sc->chunk_handler_callback) {
      sc->chunk_handler_callback (sc, chunk,
          sc->chunk_handler_callback_user_data);
    } else {
      gst_rtmp_chunk_free (chunk);
    }
  }
}

static void
gst_rtmp_connection_handle_pcm (GstRtmpConnection * connection,
    GstRtmpChunk * chunk)
{
  const guint8 *data;
  gsize size;
  guint32 moo;
  guint32 moo2;
  data = g_bytes_get_data (chunk->payload, &size);
  switch (chunk->message_type_id) {
    case GST_RTMP_MESSAGE_TYPE_SET_CHUNK_SIZE:
      moo = GST_READ_UINT32_BE (data);
      GST_INFO ("new chunk size %d", moo);
      connection->in_chunk_size = moo;
      break;
    case GST_RTMP_MESSAGE_TYPE_ABORT:
      moo = GST_READ_UINT32_BE (data);
      GST_ERROR ("unimplemented: chunk abort, stream_id = %d", moo);
      break;
    case GST_RTMP_MESSAGE_TYPE_ACKNOWLEDGEMENT:
      moo = GST_READ_UINT32_BE (data);
      /* We don't really send ack requests that we care about, so ignore */
      GST_DEBUG ("acknowledgement %d", moo);
      break;
    case GST_RTMP_MESSAGE_TYPE_USER_CONTROL:
      moo = GST_READ_UINT16_BE (data);
      moo2 = GST_READ_UINT32_BE (data + 2);
      GST_INFO ("user control: %d, %d", moo, moo2);
      gst_rtmp_connection_handle_user_control (connection, moo, moo2);
      break;
    case GST_RTMP_MESSAGE_TYPE_WINDOW_ACK_SIZE:
      moo = GST_READ_UINT32_BE (data);
      GST_INFO ("window ack size: %d", moo);
      connection->window_ack_size = GST_READ_UINT32_BE (data);
      break;
    case GST_RTMP_MESSAGE_TYPE_SET_PEER_BANDWIDTH:
      moo = GST_READ_UINT32_BE (data);
      moo2 = data[4];
      GST_DEBUG ("set peer bandwidth: %d, %d", moo, moo2);
      /* FIXME this is not correct, but close enough */
      if (connection->peer_bandwidth != moo) {
        connection->peer_bandwidth = moo;
        gst_rtmp_connection_send_window_size_request (connection);
      }
      break;
    default:
      GST_ERROR ("unimplemented protocol control, type %d",
          chunk->message_type_id);
      break;
  }
}

static void
gst_rtmp_connection_handle_user_control (GstRtmpConnection * connection,
    guint32 event_type, guint32 event_data)
{
  switch (event_type) {
    case GST_RTMP_USER_CONTROL_STREAM_BEGIN:
      GST_DEBUG ("stream begin: %d", event_data);
      break;
    case GST_RTMP_USER_CONTROL_STREAM_EOF:
      GST_ERROR ("stream EOF: %d", event_data);
      break;
    case GST_RTMP_USER_CONTROL_STREAM_DRY:
      GST_ERROR ("stream dry: %d", event_data);
      break;
    case GST_RTMP_USER_CONTROL_SET_BUFFER_LENGTH:
      GST_ERROR ("set buffer length: %d", event_data);
      break;
    case GST_RTMP_USER_CONTROL_STREAM_IS_RECORDED:
      GST_ERROR ("stream is recorded: %d", event_data);
      break;
    case GST_RTMP_USER_CONTROL_PING_REQUEST:
      GST_DEBUG ("ping request: %d", event_data);
      gst_rtmp_connection_send_ping_response (connection, event_data);
      break;
    case GST_RTMP_USER_CONTROL_PING_RESPONSE:
      GST_ERROR ("ping response: %d", event_data);
      break;
    default:
      GST_ERROR ("unimplemented: %d, %d", event_type, event_data);
      break;
  }
}

void
gst_rtmp_connection_queue_chunk (GstRtmpConnection * connection,
    GstRtmpChunk * chunk)
{
  g_return_if_fail (GST_IS_RTMP_CONNECTION (connection));
  g_return_if_fail (chunk);

  g_async_queue_push (connection->output_queue, chunk);
  gst_rtmp_connection_start_output (connection);
}

static void
gst_rtmp_connection_set_input_callback (GstRtmpConnection * connection,
    void (*input_callback) (GstRtmpConnection * connection), gsize needed_bytes)
{
  if (connection->input_callback) {
    GST_ERROR ("already an input callback");
  }
  connection->input_callback = input_callback;
  connection->input_needed_bytes = needed_bytes;
  if (needed_bytes == 0) {
    connection->input_needed_bytes = 1;
  }

  if (connection->input_callback &&
      connection->input_bytes->len >= connection->input_needed_bytes) {
    GstRtmpConnectionCallback callback;
    GST_DEBUG ("got %u bytes, calling callback", connection->input_bytes->len);
    callback = connection->input_callback;
    connection->input_callback = NULL;
    (*callback) (connection);
  }

}

void
gst_rtmp_connection_start_handshake (GstRtmpConnection * connection,
    gboolean is_server)
{
  if (connection->thread != g_thread_self ()) {
    GST_ERROR ("Called from wrong thread");
  }
  if (is_server) {
    gst_rtmp_connection_set_input_callback (connection,
        gst_rtmp_connection_server_handshake1, 1537);
  } else {
    gst_rtmp_connection_client_handshake1 (connection);
  }
}

static void
gst_rtmp_connection_client_handshake1 (GstRtmpConnection * sc)
{
  GOutputStream *os;
  guint8 *data;
  GBytes *bytes;

  os = g_io_stream_get_output_stream (G_IO_STREAM (sc->connection));

  data = g_malloc (1 + 1536);
  data[0] = 3;
  memset (data + 1, 0, 8);
  memset (data + 9, 0xa5, 1528);
  bytes = g_bytes_new_take (data, 1 + 1536);

  sc->output_bytes = bytes;
  g_output_stream_write_async (os, data, 1 + 1536,
      G_PRIORITY_DEFAULT, sc->cancellable,
      gst_rtmp_connection_client_handshake1_done, sc);
}

static void
gst_rtmp_connection_client_handshake1_done (GObject * obj,
    GAsyncResult * res, gpointer user_data)
{
  GOutputStream *os = G_OUTPUT_STREAM (obj);
  GstRtmpConnection *sc = GST_RTMP_CONNECTION (user_data);
  GError *error = NULL;
  gssize ret;

  GST_DEBUG ("gst_rtmp_connection_client_handshake1_done");

  ret = g_output_stream_write_finish (os, res, &error);
  if (ret < 1 + 1536) {
    GST_ERROR ("write error: %s", error->message);
    g_error_free (error);
    return;
  }
  GST_DEBUG ("wrote %" G_GSSIZE_FORMAT " bytes", ret);

  g_bytes_unref (sc->output_bytes);
  sc->output_bytes = NULL;

  gst_rtmp_connection_set_input_callback (sc,
      gst_rtmp_connection_client_handshake2, 1 + 1536 + 1536);
}

static void
gst_rtmp_connection_client_handshake2 (GstRtmpConnection * sc)
{
  GBytes *bytes;
  GBytes *out_bytes;
  GOutputStream *os;
  const guint8 *data;
  gsize size;

  gst_rtmp_connection_take_input_bytes (sc, 1 + 1536 + 1536, &bytes);
  out_bytes = g_bytes_new_from_bytes (bytes, 1 + 1536, 1536);
  g_bytes_unref (bytes);

  sc->output_bytes = out_bytes;
  data = g_bytes_get_data (out_bytes, &size);
  os = g_io_stream_get_output_stream (G_IO_STREAM (sc->connection));
  g_output_stream_write_async (os, data, size, G_PRIORITY_DEFAULT,
      sc->cancellable, gst_rtmp_connection_client_handshake2_done, sc);
}

static void
gst_rtmp_connection_client_handshake2_done (GObject * obj,
    GAsyncResult * res, gpointer user_data)
{
  GOutputStream *os = G_OUTPUT_STREAM (obj);
  GstRtmpConnection *sc = GST_RTMP_CONNECTION (user_data);
  GError *error = NULL;
  gssize ret;

  GST_DEBUG ("gst_rtmp_connection_client_handshake2_done");

  ret = g_output_stream_write_finish (os, res, &error);
  if (ret < 1536) {
    GST_ERROR ("write error: %s", error->message);
    g_error_free (error);
    return;
  }
  GST_DEBUG ("wrote %" G_GSSIZE_FORMAT " bytes", ret);

  g_bytes_unref (sc->output_bytes);
  sc->output_bytes = NULL;

  /* handshake finished */
  GST_INFO ("client handshake finished");
  sc->handshake_complete = TRUE;

  if (sc->input_bytes->len >= 1) {
    GST_DEBUG ("spare bytes after handshake: %u", sc->input_bytes->len);
    gst_rtmp_connection_chunk_callback (sc);
  } else {
    gst_rtmp_connection_set_input_callback (sc,
        gst_rtmp_connection_chunk_callback, 0);
  }

  gst_rtmp_connection_start_output (sc);
}

int
gst_rtmp_connection_send_command (GstRtmpConnection * connection,
    int chunk_stream_id, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    GstRtmpCommandCallback response_command, gpointer user_data)
{
  GstRtmpChunk *chunk;

  if (connection->thread != g_thread_self ()) {
    GST_ERROR ("Called from wrong thread");
  }
  chunk = gst_rtmp_chunk_new ();
  chunk->chunk_stream_id = chunk_stream_id;
  chunk->timestamp = 0;         /* FIXME */
  chunk->message_type_id = GST_RTMP_MESSAGE_TYPE_COMMAND;
  chunk->stream_id = 0;         /* FIXME */

  chunk->payload = gst_amf_serialize_command (command_name, transaction_id,
      command_object, optional_args);
  chunk->message_length = g_bytes_get_size (chunk->payload);

  gst_rtmp_connection_queue_chunk (connection, chunk);

  if (response_command) {
    CommandCallback *callback;

    callback = g_malloc0 (sizeof (CommandCallback));
    callback->chunk_stream_id = chunk_stream_id;
    callback->transaction_id = transaction_id;
    callback->func = response_command;
    callback->user_data = user_data;

    connection->command_callbacks =
        g_list_append (connection->command_callbacks, callback);
  }

  return transaction_id;
}

int
gst_rtmp_connection_send_command2 (GstRtmpConnection * connection,
    int chunk_stream_id, int stream_id,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, GstAmfNode * n3, GstAmfNode * n4,
    GstRtmpCommandCallback response_command, gpointer user_data)
{
  GstRtmpChunk *chunk;

  if (connection->thread != g_thread_self ()) {
    GST_ERROR ("Called from wrong thread");
  }
  chunk = gst_rtmp_chunk_new ();
  chunk->chunk_stream_id = chunk_stream_id;
  chunk->timestamp = 0;         /* FIXME */
  chunk->message_type_id = GST_RTMP_MESSAGE_TYPE_COMMAND;
  chunk->stream_id = stream_id;

  chunk->payload = gst_amf_serialize_command2 (command_name, transaction_id,
      command_object, optional_args, n3, n4);
  chunk->message_length = g_bytes_get_size (chunk->payload);

  gst_rtmp_connection_queue_chunk (connection, chunk);

  if (response_command) {
    CommandCallback *callback;

    callback = g_malloc0 (sizeof (CommandCallback));
    callback->chunk_stream_id = chunk_stream_id;
    callback->transaction_id = transaction_id;
    callback->func = response_command;
    callback->user_data = user_data;

    connection->command_callbacks =
        g_list_append (connection->command_callbacks, callback);
  }

  return transaction_id;
}

static void
gst_rtmp_connection_send_ack (GstRtmpConnection * connection)
{
  GstRtmpChunk *chunk;
  guint8 *data;

  chunk = gst_rtmp_chunk_new ();
  chunk->chunk_stream_id = 2;
  chunk->timestamp = 0;
  chunk->message_type_id = 5;
  chunk->stream_id = 0;

  data = g_malloc (4);
  GST_WRITE_UINT32_BE (data, connection->total_input_bytes);
  chunk->payload = g_bytes_new_take (data, 4);
  chunk->message_length = g_bytes_get_size (chunk->payload);

  gst_rtmp_connection_queue_chunk (connection, chunk);

  connection->bytes_since_ack = 0;
}

static void
gst_rtmp_connection_send_ping_response (GstRtmpConnection * connection,
    guint32 event_data)
{
  GstRtmpChunk *chunk;
  guint8 *data;

  chunk = gst_rtmp_chunk_new ();
  chunk->chunk_stream_id = 2;
  chunk->timestamp = 0;
  chunk->message_type_id = 4;
  chunk->stream_id = 0;

  data = g_malloc (8);
  GST_WRITE_UINT32_BE (data, 7);
  GST_WRITE_UINT32_BE (data + 4, event_data);
  chunk->payload = g_bytes_new_take (data, 8);
  chunk->message_length = g_bytes_get_size (chunk->payload);

  gst_rtmp_connection_queue_chunk (connection, chunk);
}

static void
gst_rtmp_connection_send_window_size_request (GstRtmpConnection * connection)
{
  GstRtmpChunk *chunk;
  guint8 *data;

  chunk = gst_rtmp_chunk_new ();
  chunk->chunk_stream_id = 2;
  chunk->timestamp = 0;
  chunk->message_type_id = 5;
  chunk->stream_id = 0;

  data = g_malloc (4);
  GST_WRITE_UINT32_BE (data, connection->peer_bandwidth);
  chunk->payload = g_bytes_new_take (data, 4);
  chunk->message_length = g_bytes_get_size (chunk->payload);

  gst_rtmp_connection_queue_chunk (connection, chunk);
}
