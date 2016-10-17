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
 * SECTION:element-gstrtmp2src
 *
 * The rtmp2src element receives input streams from an RTMP server.
 *
 * <refsect2>
 * <title>Example launch line</title>
 * |[
 * gst-launch -v rtmp2src ! decodebin ! fakesink
 * ]|
 * FIXME Describe what the pipeline does.
 * </refsect2>
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "gstrtmp2src.h"

#include <gst/gst.h>
#include <gst/base/gstpushsrc.h>
#include <rtmp/rtmpchunk.h>
#include <string.h>

GST_DEBUG_CATEGORY_STATIC (gst_rtmp2_src_debug_category);
#define GST_CAT_DEFAULT gst_rtmp2_src_debug_category

/* prototypes */

/* GObject virtual functions */
static void gst_rtmp2_src_set_property (GObject * object,
    guint property_id, const GValue * value, GParamSpec * pspec);
static void gst_rtmp2_src_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec);
static void gst_rtmp2_src_finalize (GObject * object);
static void gst_rtmp2_src_uri_handler_init (GstURIHandlerInterface * iface);

/* GstBaseSrc virtual functions */
static gboolean gst_rtmp2_src_start (GstBaseSrc * src);
static gboolean gst_rtmp2_src_stop (GstBaseSrc * src);
static gboolean gst_rtmp2_src_unlock (GstBaseSrc * src);
static GstFlowReturn gst_rtmp2_src_create (GstBaseSrc * src, guint64 offset,
    guint size, GstBuffer ** buf);

/* Internal API */
static void gst_rtmp2_src_task (gpointer user_data);
static void got_chunk (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    gpointer user_data);
static void connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void send_connect (GstRtmp2Src * src);
static void cmd_connect_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void send_create_stream (GstRtmp2Src * src);
static void create_stream_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void send_play (GstRtmp2Src * src);
static void play_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data);
static void send_secure_token_response (GstRtmp2Src * rtmp2src,
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
  PROP_USERNAME,
  PROP_PASSWORD,
};

#define DEFAULT_TIMEOUT 5
#define DEFAULT_SECURE_TOKEN ""

/* pad templates */

static GstStaticPadTemplate gst_rtmp2_src_src_template =
GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-flv")
    );


/* class initialization */

G_DEFINE_TYPE_WITH_CODE (GstRtmp2Src, gst_rtmp2_src, GST_TYPE_PUSH_SRC,
    G_IMPLEMENT_INTERFACE (GST_TYPE_URI_HANDLER,
        gst_rtmp2_src_uri_handler_init);
    G_IMPLEMENT_INTERFACE (GST_TYPE_RTMP2_URI_HANDLER, NULL);
    )

     static void gst_rtmp2_src_class_init (GstRtmp2SrcClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstBaseSrcClass *base_src_class = GST_BASE_SRC_CLASS (klass);

  /* Setting up pads and setting metadata should be moved to
     base_class_init if you intend to subclass this class. */
  gst_element_class_add_pad_template (GST_ELEMENT_CLASS (klass),
      gst_static_pad_template_get (&gst_rtmp2_src_src_template));

  gst_element_class_set_static_metadata (GST_ELEMENT_CLASS (klass),
      "RTMP source element", "Source", "Source element for RTMP streams",
      "David Schleef <ds@schleef.org>");

  gobject_class->set_property = gst_rtmp2_src_set_property;
  gobject_class->get_property = gst_rtmp2_src_get_property;
  gobject_class->finalize = gst_rtmp2_src_finalize;
  base_src_class->start = GST_DEBUG_FUNCPTR (gst_rtmp2_src_start);
  base_src_class->stop = GST_DEBUG_FUNCPTR (gst_rtmp2_src_stop);
  base_src_class->unlock = GST_DEBUG_FUNCPTR (gst_rtmp2_src_unlock);
  base_src_class->create = GST_DEBUG_FUNCPTR (gst_rtmp2_src_create);

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

  g_object_class_override_property (gobject_class, PROP_USERNAME, "username");
  g_object_class_override_property (gobject_class, PROP_PASSWORD, "password");

  GST_DEBUG_CATEGORY_INIT (gst_rtmp2_src_debug_category, "rtmp2src", 0,
      "debug category for rtmp2src element");
}

static void
gst_rtmp2_src_init (GstRtmp2Src * rtmp2src)
{
  g_mutex_init (&rtmp2src->lock);
  g_cond_init (&rtmp2src->cond);
  rtmp2src->task = gst_task_new (gst_rtmp2_src_task, rtmp2src, NULL);
  g_rec_mutex_init (&rtmp2src->task_lock);
  gst_task_set_lock (rtmp2src->task, &rtmp2src->task_lock);
  rtmp2src->client = gst_rtmp_client_new ();
  g_object_set (rtmp2src->client, "timeout", DEFAULT_TIMEOUT, NULL);
  rtmp2src->queue = g_queue_new ();
}

static void
gst_rtmp2_src_uri_handler_init (GstURIHandlerInterface * iface)
{
  gst_rtmp2_uri_handler_implement_uri_handler (iface, GST_URI_SRC);
}

void
gst_rtmp2_src_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstRtmp2Src *self = GST_RTMP2_SRC (object);

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
    case PROP_USERNAME:
      GST_OBJECT_LOCK (self);
      g_free (self->uri.username);
      self->uri.username = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PASSWORD:
      GST_OBJECT_LOCK (self);
      g_free (self->uri.password);
      self->uri.password = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp2_src_get_property (GObject * object, guint property_id,
    GValue * value, GParamSpec * pspec)
{
  GstRtmp2Src *self = GST_RTMP2_SRC (object);

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
    case PROP_USERNAME:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->uri.username);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PASSWORD:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->uri.password);
      GST_OBJECT_UNLOCK (self);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp2_src_finalize (GObject * object)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (object);

  /* clean up object here */
  gst_rtmp2_uri_clear (&rtmp2src->uri);
  g_free (rtmp2src->secure_token);
  g_object_unref (rtmp2src->task);
  g_rec_mutex_clear (&rtmp2src->task_lock);
  g_object_unref (rtmp2src->client);
  g_mutex_clear (&rtmp2src->lock);
  g_cond_clear (&rtmp2src->cond);
  g_queue_free_full (rtmp2src->queue, g_object_unref);

  G_OBJECT_CLASS (gst_rtmp2_src_parent_class)->finalize (object);
}

/* start and stop processing, ideal for opening/closing the resource */
static gboolean
gst_rtmp2_src_start (GstBaseSrc * src)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "start");

  rtmp2src->sent_header = FALSE;

  gst_task_start (rtmp2src->task);

  return TRUE;
}

static void
gst_rtmp2_src_task (gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);
  GMainLoop *main_loop;
  GMainContext *main_context;

  GST_DEBUG ("gst_rtmp2_src_task starting");

  gst_rtmp_client_set_server_address (rtmp2src->client, rtmp2src->uri.host);
  gst_rtmp_client_set_server_port (rtmp2src->client, rtmp2src->uri.port);
  gst_rtmp_client_connect_async (rtmp2src->client, NULL, connect_done,
      rtmp2src);

  main_context = g_main_context_new ();
  main_loop = g_main_loop_new (main_context, TRUE);
  rtmp2src->task_main_loop = main_loop;
  g_main_loop_run (main_loop);
  rtmp2src->task_main_loop = NULL;
  g_main_loop_unref (main_loop);

  while (g_main_context_pending (main_context)) {
    GST_ERROR ("iterating main context to clean up");
    g_main_context_iteration (main_context, FALSE);
  }

  g_main_context_unref (main_context);

  GST_DEBUG ("gst_rtmp2_src_task exiting");
}

static void
connect_done (GObject * source, GAsyncResult * result, gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);
  GError *error = NULL;
  gboolean ret;

  ret = gst_rtmp_client_connect_finish (rtmp2src->client, result, &error);
  if (!ret) {
    GST_ELEMENT_ERROR (rtmp2src, RESOURCE, OPEN_READ,
        ("Could not connect to server"), ("%s", error->message));
    g_error_free (error);
    return;
  }

  rtmp2src->connection = gst_rtmp_client_get_connection (rtmp2src->client);
  g_signal_connect (rtmp2src->connection, "got-chunk", G_CALLBACK (got_chunk),
      rtmp2src);

  send_connect (rtmp2src);
}

static void
got_chunk (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);

  if (rtmp2src->dump) {
    gst_rtmp_dump_chunk (chunk, FALSE, TRUE, TRUE);
  }

  if (chunk->stream_id != 0 &&
      (chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_VIDEO ||
          (chunk->message_type_id == GST_RTMP_MESSAGE_TYPE_DATA
              && chunk->message_length > 100))) {
    g_object_ref (chunk);
    g_mutex_lock (&rtmp2src->lock);
    g_queue_push_tail (rtmp2src->queue, chunk);
    g_cond_signal (&rtmp2src->cond);
    g_mutex_unlock (&rtmp2src->lock);
  }
}

static void
send_connect (GstRtmp2Src * rtmp2src)
{
  GstAmfNode *node;
  gchar *uri;

  node = gst_amf_node_new (GST_AMF_TYPE_OBJECT);
  gst_amf_object_set_string (node, "app", rtmp2src->uri.application);
  gst_amf_object_set_string (node, "type", "nonprivate");
  uri = gst_rtmp2_uri_get_string (&rtmp2src->uri, FALSE);
  gst_amf_object_set_string (node, "tcUrl", uri);
  g_free (uri);
  // "fpad": False,
  // "capabilities": 15,
  // "audioCodecs": 3191,
  // "videoCodecs": 252,
  // "videoFunction": 1,
  gst_rtmp_connection_send_command (rtmp2src->connection, 3, "connect", 1, node,
      NULL, cmd_connect_done, rtmp2src);
  gst_amf_node_free (node);
}

static void
cmd_connect_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);
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
      send_secure_token_response (rtmp2src, challenge);
    }

    send_create_stream (rtmp2src);
  } else {
    GST_ERROR ("connect error");
  }
}

static void
send_create_stream (GstRtmp2Src * rtmp2src)
{
  GstAmfNode *node;

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  gst_rtmp_connection_send_command (rtmp2src->connection, 3, "createStream", 2,
      node, NULL, create_stream_done, rtmp2src);
  gst_amf_node_free (node);

}

static void
create_stream_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);
  gboolean ret;
  int stream_id;

  ret = FALSE;
  if (optional_args) {
    stream_id = gst_amf_node_get_number (optional_args);
    ret = TRUE;
  }

  if (ret) {
    GST_DEBUG ("createStream success, stream_id=%d", stream_id);
    send_play (rtmp2src);
  } else {
    GST_ERROR ("createStream failed");
  }
}

static void
send_play (GstRtmp2Src * rtmp2src)
{
  GstAmfNode *n1;
  GstAmfNode *n2;
  GstAmfNode *n3;

  n1 = gst_amf_node_new (GST_AMF_TYPE_NULL);
  n2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (n2, rtmp2src->uri.stream);
  n3 = gst_amf_node_new (GST_AMF_TYPE_NUMBER);
  gst_amf_node_set_number (n3, 0);
  gst_rtmp_connection_send_command2 (rtmp2src->connection, 8, 1, "play", 3, n1,
      n2, n3, NULL, play_done, rtmp2src);
  gst_amf_node_free (n1);
  gst_amf_node_free (n2);
  gst_amf_node_free (n3);

}

static void
play_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  //GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);
  gboolean ret;
  int stream_id;

  ret = FALSE;
  if (optional_args) {
    stream_id = gst_amf_node_get_number (optional_args);
    ret = TRUE;
  }

  if (ret) {
    GST_DEBUG ("play success, stream_id=%d", stream_id);
  } else {
    GST_ERROR ("play failed");
  }
}

static gboolean
gst_rtmp2_src_stop (GstBaseSrc * src)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "stop");

  gst_rtmp_connection_close (rtmp2src->connection);

  gst_task_stop (rtmp2src->task);
  g_main_loop_quit (rtmp2src->task_main_loop);

  gst_task_join (rtmp2src->task);

  return TRUE;
}

/* unlock any pending access to the resource. subclasses should unlock
 * any function ASAP. */
static gboolean
gst_rtmp2_src_unlock (GstBaseSrc * src)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "unlock");

  g_mutex_lock (&rtmp2src->lock);
  rtmp2src->reset = TRUE;
  g_cond_signal (&rtmp2src->cond);
  g_mutex_unlock (&rtmp2src->lock);

  return TRUE;
}

/* ask the subclass to create a buffer with offset and size, the default
 * implementation will call alloc and fill. */
static GstFlowReturn
gst_rtmp2_src_create (GstBaseSrc * src, guint64 offset, guint size,
    GstBuffer ** buf)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);
  GstRtmpChunk *chunk;
  const char *data;
  guint8 *buf_data;
  gsize payload_size;

  GST_DEBUG_OBJECT (rtmp2src, "create");

  if (!rtmp2src->sent_header) {
    static const guint8 header[] = {
      0x46, 0x4c, 0x56, 0x01, 0x01, 0x00, 0x00, 0x00,
      0x09, 0x00, 0x00, 0x00, 0x00,
    };
    guint8 *data;

    rtmp2src->sent_header = TRUE;
    data = g_memdup (header, sizeof (header));
    data[4] = 0x1;              /* |4 with audio */
    *buf = gst_buffer_new_wrapped (data, sizeof (header));
    return GST_FLOW_OK;
  }

  g_mutex_lock (&rtmp2src->lock);
  chunk = g_queue_pop_head (rtmp2src->queue);
  while (!chunk) {
    if (rtmp2src->reset) {
      g_mutex_unlock (&rtmp2src->lock);
      return GST_FLOW_FLUSHING;
    }
    g_cond_wait (&rtmp2src->cond, &rtmp2src->lock);
    chunk = g_queue_pop_head (rtmp2src->queue);
  }
  g_mutex_unlock (&rtmp2src->lock);

  data = g_bytes_get_data (chunk->payload, &payload_size);

  buf_data = g_malloc (payload_size + 11 + 4);
  buf_data[0] = chunk->message_type_id;
  GST_WRITE_UINT24_BE (buf_data + 1, chunk->message_length);
  GST_WRITE_UINT24_BE (buf_data + 4, chunk->timestamp);
  GST_WRITE_UINT24_BE (buf_data + 7, 0);
  memcpy (buf_data + 11, data, payload_size);
  GST_WRITE_UINT32_BE (buf_data + payload_size + 11, payload_size + 11);
  g_object_unref (chunk);

  *buf = gst_buffer_new_wrapped (buf_data, payload_size + 11 + 4);

  return GST_FLOW_OK;
}

/* internal API */

static void
send_secure_token_response (GstRtmp2Src * rtmp2src, const char *challenge)
{
  GstAmfNode *node1;
  GstAmfNode *node2;
  gchar *response;

  if (rtmp2src->secure_token == NULL || !rtmp2src->secure_token[0]) {
    GST_ELEMENT_ERROR (rtmp2src, RESOURCE, OPEN_READ,
        ("Server requested secureToken authentication"), (NULL));
    return;
  }

  response = gst_rtmp_tea_decode (rtmp2src->secure_token, challenge);

  GST_DEBUG ("response: %s", response);

  node1 = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string_take (node2, response);

  gst_rtmp_connection_send_command (rtmp2src->connection, 3,
      "secureTokenResponse", 0, node1, node2, NULL, NULL);
  gst_amf_node_free (node1);
  gst_amf_node_free (node2);

}
