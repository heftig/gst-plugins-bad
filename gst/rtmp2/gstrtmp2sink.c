/* GStreamer
 * Copyright (C) 2014 David Schleef <ds@schleef.org>
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
/**
 * SECTION:element-gstrtmp2sink
 *
 * The rtmp2sink element sends audio and video streams to an RTMP
 * server.
 *
 * <refsect2>
 * <title>Example launch line</title>
 * |[
 * gst-launch -v videotestsrc ! x264enc ! flvmux ! rtmp2sink
 *     location=rtmp://server.example.com/live/myStream
 * ]|
 * FIXME Describe what the pipeline does.
 * </refsect2>
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "gstrtmp2sink.h"

#include <gst/gst.h>
#include <gst/base/gstbasesink.h>
#include <string.h>

GST_DEBUG_CATEGORY_STATIC (gst_rtmp2_sink_debug_category);
#define GST_CAT_DEFAULT gst_rtmp2_sink_debug_category

/* prototypes */

/* GObject virtual functions */
static void gst_rtmp2_sink_set_property (GObject * object,
    guint property_id, const GValue * value, GParamSpec * pspec);
static void gst_rtmp2_sink_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec);
static void gst_rtmp2_sink_finalize (GObject * object);
static void gst_rtmp2_sink_uri_handler_init (GstURIHandlerInterface * iface);

/* GstBaseSink virtual functions */
static gboolean gst_rtmp2_sink_start (GstBaseSink * sink);
static gboolean gst_rtmp2_sink_stop (GstBaseSink * sink);
static gboolean gst_rtmp2_sink_unlock (GstBaseSink * sink);
static GstFlowReturn gst_rtmp2_sink_preroll (GstBaseSink * sink,
    GstBuffer * buffer);
static GstFlowReturn gst_rtmp2_sink_render (GstBaseSink * sink,
    GstBuffer * buffer);

/* Internal API */
static void gst_rtmp2_sink_task (gpointer user_data);
static void connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void send_connect (GstRtmp2Sink * rtmp2sink);
static void cmd_connect_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void send_create_stream (GstRtmp2Sink * rtmp2sink);
static void create_stream_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void send_publish (GstRtmp2Sink * rtmp2sink);
static void publish_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void got_chunk (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    gpointer user_data);
static void send_secure_token_response (GstRtmp2Sink * rtmp2sink,
    const char *challenge);

enum
{
  PROP_0,
  PROP_LOCATION,
  PROP_HOST,
  PROP_PORT,
  PROP_APPLICATION,
  PROP_STREAM,
  PROP_SECURE_TOKEN,
};

#define DEFAULT_TIMEOUT 5
#define DEFAULT_SECURE_TOKEN ""

/* pad templates */

static GstStaticPadTemplate gst_rtmp2_sink_sink_template =
GST_STATIC_PAD_TEMPLATE ("sink",
    GST_PAD_SINK,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-flv")
    );


/* class initialization */

G_DEFINE_TYPE_WITH_CODE (GstRtmp2Sink, gst_rtmp2_sink, GST_TYPE_BASE_SINK,
    G_IMPLEMENT_INTERFACE (GST_TYPE_URI_HANDLER,
        gst_rtmp2_sink_uri_handler_init);
    G_IMPLEMENT_INTERFACE (GST_TYPE_RTMP2_URI_HANDLER, NULL);
    )

     static void gst_rtmp2_sink_class_init (GstRtmp2SinkClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstBaseSinkClass *base_sink_class = GST_BASE_SINK_CLASS (klass);

  /* Setting up pads and setting metadata should be moved to
     base_class_init if you intend to subclass this class. */
  gst_element_class_add_pad_template (GST_ELEMENT_CLASS (klass),
      gst_static_pad_template_get (&gst_rtmp2_sink_sink_template));

  gst_element_class_set_static_metadata (GST_ELEMENT_CLASS (klass),
      "RTMP sink element", "Sink", "Sink element for RTMP streams",
      "David Schleef <ds@schleef.org>");

  gobject_class->set_property = gst_rtmp2_sink_set_property;
  gobject_class->get_property = gst_rtmp2_sink_get_property;
  gobject_class->finalize = gst_rtmp2_sink_finalize;
  base_sink_class->start = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_start);
  base_sink_class->stop = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_stop);
  base_sink_class->unlock = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_unlock);
  base_sink_class->preroll = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_preroll);
  base_sink_class->render = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_render);

  g_object_class_override_property (gobject_class, PROP_LOCATION, "location");
  g_object_class_override_property (gobject_class, PROP_HOST, "host");
  g_object_class_override_property (gobject_class, PROP_PORT, "port");
  g_object_class_override_property (gobject_class, PROP_APPLICATION,
      "application");
  g_object_class_override_property (gobject_class, PROP_STREAM, "stream");

  g_object_class_install_property (gobject_class, PROP_SECURE_TOKEN,
      g_param_spec_string ("secure-token", "Secure token",
          "Secure token used for authentication",
          DEFAULT_SECURE_TOKEN,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  GST_DEBUG_CATEGORY_INIT (gst_rtmp2_sink_debug_category, "rtmp2sink", 0,
      "debug category for rtmp2sink element");
}

static void
gst_rtmp2_sink_init (GstRtmp2Sink * rtmp2sink)
{
  g_mutex_init (&rtmp2sink->lock);
  g_cond_init (&rtmp2sink->cond);
  rtmp2sink->task = gst_task_new (gst_rtmp2_sink_task, rtmp2sink, NULL);
  g_rec_mutex_init (&rtmp2sink->task_lock);
  gst_task_set_lock (rtmp2sink->task, &rtmp2sink->task_lock);
  rtmp2sink->client = gst_rtmp_client_new ();
  g_object_set (rtmp2sink->client, "timeout", DEFAULT_TIMEOUT, NULL);
}

static void
gst_rtmp2_sink_uri_handler_init (GstURIHandlerInterface * iface)
{
  gst_rtmp2_uri_handler_implement_uri_handler (iface, GST_URI_SINK);
}

void
gst_rtmp2_sink_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstRtmp2Sink *self = GST_RTMP2_SINK (object);

  switch (property_id) {
    case PROP_LOCATION:
      gst_rtmp2_uri_handler_set_uri (GST_RTMP2_URI_HANDLER (self),
          g_value_get_string (value));
      break;
    case PROP_HOST:
      GST_OBJECT_LOCK (self);
      g_free (self->uri.host);
      self->uri.host = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PORT:
      GST_OBJECT_LOCK (self);
      self->uri.port = g_value_get_int (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_APPLICATION:
      GST_OBJECT_LOCK (self);
      g_free (self->uri.application);
      self->uri.application = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_STREAM:
      GST_OBJECT_LOCK (self);
      g_free (self->uri.stream);
      self->uri.stream = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_SECURE_TOKEN:
      GST_OBJECT_LOCK (self);
      g_free (self->secure_token);
      self->secure_token = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp2_sink_get_property (GObject * object, guint property_id,
    GValue * value, GParamSpec * pspec)
{
  GstRtmp2Sink *self = GST_RTMP2_SINK (object);

  switch (property_id) {
    case PROP_LOCATION:
      GST_OBJECT_LOCK (self);
      g_value_take_string (value, gst_rtmp2_uri_get_string (&self->uri, TRUE));
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_HOST:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->uri.host);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PORT:
      GST_OBJECT_LOCK (self);
      g_value_set_int (value, self->uri.port);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_APPLICATION:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->uri.application);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_STREAM:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->uri.stream);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_SECURE_TOKEN:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->secure_token);
      GST_OBJECT_UNLOCK (self);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp2_sink_finalize (GObject * object)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (object);

  /* clean up object here */
  gst_rtmp2_uri_clear (&rtmp2sink->uri);
  g_free (rtmp2sink->secure_token);
  g_object_unref (rtmp2sink->task);
  g_rec_mutex_clear (&rtmp2sink->task_lock);
  g_object_unref (rtmp2sink->client);
  g_mutex_clear (&rtmp2sink->lock);
  g_cond_clear (&rtmp2sink->cond);

  G_OBJECT_CLASS (gst_rtmp2_sink_parent_class)->finalize (object);
}

/* start and stop processing, ideal for opening/closing the resource */
static gboolean
gst_rtmp2_sink_start (GstBaseSink * sink)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);

  GST_DEBUG_OBJECT (rtmp2sink, "start");

  gst_task_start (rtmp2sink->task);

  return TRUE;
}

static gboolean
gst_rtmp2_sink_stop (GstBaseSink * sink)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);

  GST_DEBUG_OBJECT (rtmp2sink, "stop");

  gst_rtmp_connection_close (rtmp2sink->connection);

  gst_task_stop (rtmp2sink->task);
  g_main_loop_quit (rtmp2sink->task_main_loop);

  gst_task_join (rtmp2sink->task);

  return TRUE;
}

/* unlock any pending access to the resource. subclasses should unlock
 * any function ASAP. */
static gboolean
gst_rtmp2_sink_unlock (GstBaseSink * sink)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);

  GST_DEBUG_OBJECT (rtmp2sink, "unlock");

  g_mutex_lock (&rtmp2sink->lock);
  rtmp2sink->reset = TRUE;
  g_cond_signal (&rtmp2sink->cond);
  g_mutex_unlock (&rtmp2sink->lock);

  return TRUE;
}

static GstFlowReturn
gst_rtmp2_sink_preroll (GstBaseSink * sink, GstBuffer * buffer)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);

  g_mutex_lock (&rtmp2sink->lock);
  while (!rtmp2sink->is_connected) {
    g_cond_wait (&rtmp2sink->cond, &rtmp2sink->lock);
  }
  g_mutex_unlock (&rtmp2sink->lock);

  return GST_FLOW_OK;
}

static GstFlowReturn
gst_rtmp2_sink_render (GstBaseSink * sink, GstBuffer * buffer)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);
  GstRtmpChunk *chunk;
  GBytes *bytes;
  gsize size;
  guint8 *data;

  GST_DEBUG_OBJECT (rtmp2sink, "render");

  size = gst_buffer_get_size (buffer);
  gst_buffer_extract_dup (buffer, 0, size, (gpointer *) & data, &size);

  if (size >= 4) {
    if (data[0] == 'F' && data[1] == 'L' && data[2] == 'V') {
      /* drop the header, we don't need it */
      g_free (data);
      return GST_FLOW_OK;
    }
  }

  if (size < 15) {
    g_free (data);
    return GST_FLOW_ERROR;
  }


  chunk = gst_rtmp_chunk_new ();
  chunk->message_type_id = data[0];
  chunk->chunk_stream_id = 4;
  if (chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_DATA ||
      chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_AUDIO ||
      chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_VIDEO) {
  } else {
    GST_ERROR ("unknown message_type_id %d", chunk->message_type_id);
  }
  chunk->message_length = GST_READ_UINT24_BE (data + 1);
  chunk->timestamp = GST_READ_UINT24_BE (data + 4);
  chunk->stream_id = 1;         /* FIXME use actual stream id */

  if (chunk->message_length != size - 15) {
    GST_ERROR ("message length was %" G_GSIZE_FORMAT " expected %"
        G_GSIZE_FORMAT, chunk->message_length, size - 15);
  }

  if (chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_DATA) {
    static const guint8 header[] = {
      0x02, 0x00, 0x0d, 0x40, 0x73, 0x65, 0x74, 0x44,
      0x61, 0x74, 0x61, 0x46, 0x72, 0x61, 0x6d, 0x65
    };
    guint8 *newdata;
    /* FIXME HACK, attach a setDataFrame header.  This should be done
     * using a command. */

    newdata = g_malloc (size - 15 + sizeof (header));
    memcpy (newdata, header, sizeof (header));
    memcpy (newdata + sizeof (header), data + 11, size - 15);
    g_free (data);

    chunk->message_length += sizeof (header);
    chunk->payload = g_bytes_new_take (newdata, chunk->message_length);
  } else {
    bytes = g_bytes_new_take (data, size);
    chunk->payload = g_bytes_new_from_bytes (bytes, 11, size - 15);
    g_bytes_unref (bytes);
  }

  if (rtmp2sink->dump) {
    gst_rtmp_dump_chunk (chunk, TRUE, TRUE, TRUE);
  }

  gst_rtmp_connection_queue_chunk (rtmp2sink->connection, chunk);

  return GST_FLOW_OK;
}

/* Internal API */

static void
gst_rtmp2_sink_task (gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (user_data);
  GMainLoop *main_loop;
  GMainContext *main_context;

  GST_DEBUG ("gst_rtmp2_sink_task starting");

  gst_rtmp_client_set_server_address (rtmp2sink->client, rtmp2sink->uri.host);
  gst_rtmp_client_set_server_port (rtmp2sink->client, rtmp2sink->uri.port);
  gst_rtmp_client_connect_async (rtmp2sink->client, NULL, connect_done,
      rtmp2sink);

  main_context = g_main_context_new ();
  main_loop = g_main_loop_new (main_context, TRUE);
  rtmp2sink->task_main_loop = main_loop;
  g_main_loop_run (main_loop);
  rtmp2sink->task_main_loop = NULL;
  g_main_loop_unref (main_loop);

  while (g_main_context_pending (main_context)) {
    GST_ERROR ("iterating main context to clean up");
    g_main_context_iteration (main_context, FALSE);
  }

  g_main_context_unref (main_context);

  GST_DEBUG ("gst_rtmp2_sink_task exiting");
}

static void
connect_done (GObject * source, GAsyncResult * result, gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (user_data);
  GError *error = NULL;
  gboolean ret;

  ret = gst_rtmp_client_connect_finish (rtmp2sink->client, result, &error);
  if (!ret) {
    GST_ELEMENT_ERROR (rtmp2sink, RESOURCE, OPEN_READ,
        ("Could not connect to server"), ("%s", error->message));
    g_error_free (error);
    return;
  }

  rtmp2sink->connection = gst_rtmp_client_get_connection (rtmp2sink->client);
  g_signal_connect (rtmp2sink->connection, "got-chunk", G_CALLBACK (got_chunk),
      rtmp2sink);

  send_connect (rtmp2sink);
}

static void
send_connect (GstRtmp2Sink * rtmp2sink)
{
  GstAmfNode *node;
  gchar *uri;

  node = gst_amf_node_new (GST_AMF_TYPE_OBJECT);
  gst_amf_object_set_string (node, "app", rtmp2sink->uri.application);
  gst_amf_object_set_string (node, "type", "nonprivate");
  uri = gst_rtmp2_uri_get_string (&rtmp2sink->uri, FALSE);
  gst_amf_object_set_string (node, "tcUrl", uri);
  g_free (uri);
  // "fpad": False,
  // "capabilities": 15,
  // "audioCodecs": 3191,
  // "videoCodecs": 252,
  // "videoFunction": 1,
  gst_rtmp_connection_send_command (rtmp2sink->connection, 3, "connect", 1,
      node, NULL, cmd_connect_done, rtmp2sink);
  gst_amf_node_free (node);
}

static void
cmd_connect_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (user_data);
  gboolean ret;

  ret = FALSE;
  if (optional_args) {
    const GstAmfNode *n;
    n = gst_amf_node_get_object (optional_args, "code");
    if (n) {
      const char *s;
      s = gst_amf_node_get_string (n);
      if (g_strcmp0 (s, "NetConnection.Connect.Success") == 0) {
        ret = TRUE;
      }
    }
  }

  if (ret) {
    const GstAmfNode *n;

    GST_DEBUG ("success");

    n = gst_amf_node_get_object (optional_args, "secureToken");
    if (n) {
      const gchar *challenge;
      challenge = gst_amf_node_get_string (n);
      GST_DEBUG ("secureToken challenge: %s", challenge);
      send_secure_token_response (rtmp2sink, challenge);
    }

    send_create_stream (rtmp2sink);
  } else {
    GST_ERROR ("connect error");
  }
}

static void
send_create_stream (GstRtmp2Sink * rtmp2sink)
{
  GstAmfNode *node;
  GstAmfNode *node2;

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, rtmp2sink->uri.stream);
  gst_rtmp_connection_send_command (rtmp2sink->connection, 3, "releaseStream",
      2, node, node2, NULL, NULL);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, rtmp2sink->uri.stream);
  gst_rtmp_connection_send_command (rtmp2sink->connection, 3, "FCPublish", 3,
      node, node2, NULL, NULL);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  gst_rtmp_connection_send_command (rtmp2sink->connection, 3, "createStream", 4,
      node, NULL, create_stream_done, rtmp2sink);
  gst_amf_node_free (node);
}

static void
create_stream_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (user_data);
  gboolean ret;
  int stream_id;

  ret = FALSE;
  if (optional_args) {
    stream_id = gst_amf_node_get_number (optional_args);
    ret = TRUE;
  }

  if (ret) {
    GST_DEBUG ("createStream success, stream_id=%d", stream_id);
    send_publish (rtmp2sink);
  } else {
    GST_ERROR ("createStream failed");
  }
}

static void
send_publish (GstRtmp2Sink * rtmp2sink)
{
  GstAmfNode *node;
  GstAmfNode *node2;
  GstAmfNode *node3;

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, rtmp2sink->uri.stream);
  node3 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node3, rtmp2sink->uri.application);
  gst_rtmp_connection_send_command2 (rtmp2sink->connection, 4, 1, "publish", 5,
      node, node2, node3, NULL, publish_done, rtmp2sink);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);
  gst_amf_node_free (node3);
}

static void
publish_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (user_data);
  gboolean ret;
  int stream_id;

  ret = FALSE;
  if (optional_args) {
    stream_id = gst_amf_node_get_number (optional_args);
    ret = TRUE;
  }

  if (ret) {
    GST_DEBUG ("publish success, stream_id=%d", stream_id);

    g_mutex_lock (&rtmp2sink->lock);
    rtmp2sink->is_connected = TRUE;
    g_cond_signal (&rtmp2sink->cond);
    g_mutex_unlock (&rtmp2sink->lock);
  } else {
    GST_ERROR ("publish failed");
  }
}

static void
got_chunk (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (user_data);

  if (rtmp2sink->dump) {
    gst_rtmp_dump_chunk (chunk, FALSE, TRUE, TRUE);
  }
}

static void
send_secure_token_response (GstRtmp2Sink * rtmp2sink, const char *challenge)
{
  GstAmfNode *node1;
  GstAmfNode *node2;
  gchar *response;

  if (rtmp2sink->secure_token == NULL || !rtmp2sink->secure_token[0]) {
    GST_ELEMENT_ERROR (rtmp2sink, RESOURCE, OPEN_READ,
        ("Server requested secureToken authentication"), (NULL));
    return;
  }

  response = gst_rtmp_tea_decode (rtmp2sink->secure_token, challenge);

  GST_DEBUG ("response: %s", response);

  node1 = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string_take (node2, response);

  gst_rtmp_connection_send_command (rtmp2sink->connection, 3,
      "secureTokenResponse", 0, node1, node2, NULL, NULL);
  gst_amf_node_free (node1);
  gst_amf_node_free (node2);

}
