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

#include <gst/gst.h>
#include <gst/base/gstbasesrc.h>
#include <gst/base/gstpushsrc.h>
#include "gstrtmp2src.h"
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
static void gst_rtmp2_src_dispose (GObject * object);
static void gst_rtmp2_src_finalize (GObject * object);
static void gst_rtmp2_src_uri_handler_init (gpointer g_iface,
    gpointer iface_data);

/* GstBaseSrc virtual functions */
static gboolean gst_rtmp2_src_start (GstBaseSrc * src);
static gboolean gst_rtmp2_src_stop (GstBaseSrc * src);
static void gst_rtmp2_src_get_times (GstBaseSrc * src, GstBuffer * buffer,
    GstClockTime * start, GstClockTime * end);
static gboolean gst_rtmp2_src_get_size (GstBaseSrc * src, guint64 * size);
static gboolean gst_rtmp2_src_is_seekable (GstBaseSrc * src);
static gboolean gst_rtmp2_src_prepare_seek_segment (GstBaseSrc * src,
    GstEvent * seek, GstSegment * segment);
static gboolean gst_rtmp2_src_do_seek (GstBaseSrc * src, GstSegment * segment);
static gboolean gst_rtmp2_src_unlock (GstBaseSrc * src);
static gboolean gst_rtmp2_src_unlock_stop (GstBaseSrc * src);
static gboolean gst_rtmp2_src_query (GstBaseSrc * src, GstQuery * query);
static gboolean gst_rtmp2_src_event (GstBaseSrc * src, GstEvent * event);
static GstFlowReturn gst_rtmp2_src_create (GstBaseSrc * src, guint64 offset,
    guint size, GstBuffer ** buf);
static GstFlowReturn gst_rtmp2_src_alloc (GstBaseSrc * src, guint64 offset,
    guint size, GstBuffer ** buf);

/* URI handler */
static GstURIType gst_rtmp2_src_uri_get_type (GType type);
static const gchar *const *gst_rtmp2_src_uri_get_protocols (GType type);
static gchar *gst_rtmp2_src_uri_get_uri (GstURIHandler * handler);
static gboolean gst_rtmp2_src_uri_set_uri (GstURIHandler * handler,
    const gchar * uri, GError ** error);

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

static gchar *gst_rtmp2_src_get_uri (GstRtmp2Src * src);
static gboolean gst_rtmp2_src_set_uri (GstRtmp2Src * src, const char *uri);

enum
{
  PROP_0,
  PROP_LOCATION,
  PROP_TIMEOUT,
  PROP_SERVER_ADDRESS,
  PROP_PORT,
  PROP_APPLICATION,
  PROP_STREAM,
  PROP_SECURE_TOKEN
};

#define DEFAULT_LOCATION "rtmp://localhost/live/myStream"
#define DEFAULT_TIMEOUT 5
#define DEFAULT_SERVER_ADDRESS ""
#define DEFAULT_PORT 1935
#define DEFAULT_APPLICATION "live"
#define DEFAULT_STREAM "myStream"
#define DEFAULT_SECURE_TOKEN ""

/* pad templates */

static GstStaticPadTemplate gst_rtmp2_src_src_template =
GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-flv")
    );


/* class initialization */

static void
do_init (GType g_define_type_id)
{
  G_IMPLEMENT_INTERFACE (GST_TYPE_URI_HANDLER, gst_rtmp2_src_uri_handler_init);
  GST_DEBUG_CATEGORY_INIT (gst_rtmp2_src_debug_category, "rtmp2src", 0,
      "debug category for rtmp2src element");
}

G_DEFINE_TYPE_WITH_CODE (GstRtmp2Src, gst_rtmp2_src, GST_TYPE_PUSH_SRC,
    do_init (g_define_type_id))

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
  gobject_class->dispose = gst_rtmp2_src_dispose;
  gobject_class->finalize = gst_rtmp2_src_finalize;
  base_src_class->start = GST_DEBUG_FUNCPTR (gst_rtmp2_src_start);
  base_src_class->stop = GST_DEBUG_FUNCPTR (gst_rtmp2_src_stop);
  base_src_class->get_times = GST_DEBUG_FUNCPTR (gst_rtmp2_src_get_times);
  base_src_class->get_size = GST_DEBUG_FUNCPTR (gst_rtmp2_src_get_size);
  base_src_class->is_seekable = GST_DEBUG_FUNCPTR (gst_rtmp2_src_is_seekable);
  base_src_class->prepare_seek_segment =
      GST_DEBUG_FUNCPTR (gst_rtmp2_src_prepare_seek_segment);
  base_src_class->do_seek = GST_DEBUG_FUNCPTR (gst_rtmp2_src_do_seek);
  base_src_class->unlock = GST_DEBUG_FUNCPTR (gst_rtmp2_src_unlock);
  base_src_class->unlock_stop = GST_DEBUG_FUNCPTR (gst_rtmp2_src_unlock_stop);
  if (0)
    base_src_class->query = GST_DEBUG_FUNCPTR (gst_rtmp2_src_query);
  base_src_class->event = GST_DEBUG_FUNCPTR (gst_rtmp2_src_event);
  base_src_class->create = GST_DEBUG_FUNCPTR (gst_rtmp2_src_create);
  base_src_class->alloc = GST_DEBUG_FUNCPTR (gst_rtmp2_src_alloc);

  g_object_class_install_property (gobject_class, PROP_LOCATION,
      g_param_spec_string ("location", "RTMP Location",
          "Location of the RTMP url to read",
          DEFAULT_LOCATION, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_SERVER_ADDRESS,
      g_param_spec_string ("server-address", "RTMP Server Address",
          "Address of RTMP server",
          DEFAULT_SERVER_ADDRESS, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_PORT,
      g_param_spec_int ("port", "RTMP server port",
          "RTMP server port (usually 1935)",
          1, 65535, DEFAULT_PORT, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_APPLICATION,
      g_param_spec_string ("application", "RTMP application",
          "RTMP application",
          DEFAULT_APPLICATION, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_STREAM,
      g_param_spec_string ("stream", "RTMP stream",
          "RTMP stream",
          DEFAULT_STREAM, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_SECURE_TOKEN,
      g_param_spec_string ("secure-token", "Secure token",
          "Secure token used for authentication",
          DEFAULT_SECURE_TOKEN, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

}

static void
gst_rtmp2_src_init (GstRtmp2Src * rtmp2src)
{
  g_mutex_init (&rtmp2src->lock);
  g_cond_init (&rtmp2src->cond);
  rtmp2src->queue = g_queue_new ();

  //gst_base_src_set_live (GST_BASE_SRC(rtmp2src), TRUE);

  rtmp2src->timeout = DEFAULT_TIMEOUT;
  gst_rtmp2_src_set_uri (rtmp2src, DEFAULT_LOCATION);
  rtmp2src->secure_token = g_strdup (DEFAULT_SECURE_TOKEN);

  rtmp2src->task = gst_task_new (gst_rtmp2_src_task, rtmp2src, NULL);
  g_rec_mutex_init (&rtmp2src->task_lock);
  gst_task_set_lock (rtmp2src->task, &rtmp2src->task_lock);
  rtmp2src->client = gst_rtmp_client_new ();
  g_object_set (rtmp2src->client, "timeout", rtmp2src->timeout, NULL);

}

static void
gst_rtmp2_src_uri_handler_init (gpointer g_iface, gpointer iface_data)
{
  GstURIHandlerInterface *iface = (GstURIHandlerInterface *) g_iface;

  iface->get_type = gst_rtmp2_src_uri_get_type;
  iface->get_protocols = gst_rtmp2_src_uri_get_protocols;
  iface->get_uri = gst_rtmp2_src_uri_get_uri;
  iface->set_uri = gst_rtmp2_src_uri_set_uri;
}

void
gst_rtmp2_src_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (object);

  GST_DEBUG_OBJECT (rtmp2src, "set_property");

  switch (property_id) {
    case PROP_LOCATION:
      gst_rtmp2_src_set_uri (rtmp2src, g_value_get_string (value));
      break;
    case PROP_SERVER_ADDRESS:
      g_free (rtmp2src->server_address);
      rtmp2src->server_address = g_value_dup_string (value);
      break;
    case PROP_PORT:
      rtmp2src->port = g_value_get_int (value);
      break;
    case PROP_APPLICATION:
      g_free (rtmp2src->application);
      rtmp2src->application = g_value_dup_string (value);
      break;
    case PROP_STREAM:
      g_free (rtmp2src->stream);
      rtmp2src->stream = g_value_dup_string (value);
      break;
    case PROP_SECURE_TOKEN:
      g_free (rtmp2src->secure_token);
      rtmp2src->secure_token = g_value_dup_string (value);
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
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (object);

  GST_DEBUG_OBJECT (rtmp2src, "get_property");

  switch (property_id) {
    case PROP_LOCATION:
      g_value_set_string (value, gst_rtmp2_src_get_uri (rtmp2src));
      break;
    case PROP_SERVER_ADDRESS:
      g_value_set_string (value, rtmp2src->server_address);
      break;
    case PROP_PORT:
      g_value_set_int (value, rtmp2src->port);
      break;
    case PROP_APPLICATION:
      g_value_set_string (value, rtmp2src->application);
      break;
    case PROP_STREAM:
      g_value_set_string (value, rtmp2src->stream);
      break;
    case PROP_SECURE_TOKEN:
      g_value_set_string (value, rtmp2src->secure_token);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp2_src_dispose (GObject * object)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (object);

  GST_DEBUG_OBJECT (rtmp2src, "dispose");

  /* clean up as possible.  may be called multiple times */

  G_OBJECT_CLASS (gst_rtmp2_src_parent_class)->dispose (object);
}

void
gst_rtmp2_src_finalize (GObject * object)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (object);

  GST_DEBUG_OBJECT (rtmp2src, "finalize");

  /* clean up object here */
  g_free (rtmp2src->uri);
  g_free (rtmp2src->server_address);
  g_free (rtmp2src->application);
  g_free (rtmp2src->stream);
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

  gst_rtmp_client_set_server_address (rtmp2src->client,
      rtmp2src->server_address);
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
  gst_amf_object_set_string (node, "app", rtmp2src->application);
  gst_amf_object_set_string (node, "type", "nonprivate");
  uri = gst_rtmp2_src_get_uri (rtmp2src);
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
      if (strcmp (s, "NetConnection.Connect.Success") == 0) {
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
  gst_amf_node_set_string (n2, rtmp2src->stream);
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

/* given a buffer, return start and stop time when it should be pushed
 * out. The base class will sync on the clock using these times. */
static void
gst_rtmp2_src_get_times (GstBaseSrc * src, GstBuffer * buffer,
    GstClockTime * start, GstClockTime * end)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "get_times");

}

/* get the total size of the resource in bytes */
static gboolean
gst_rtmp2_src_get_size (GstBaseSrc * src, guint64 * size)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "get_size");

  return TRUE;
}

/* check if the resource is seekable */
static gboolean
gst_rtmp2_src_is_seekable (GstBaseSrc * src)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "is_seekable");

  return FALSE;
}

/* Prepare the segment on which to perform do_seek(), converting to the
 * current basesrc format. */
static gboolean
gst_rtmp2_src_prepare_seek_segment (GstBaseSrc * src, GstEvent * seek,
    GstSegment * segment)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "prepare_seek_segment");

  return TRUE;
}

/* notify subclasses of a seek */
static gboolean
gst_rtmp2_src_do_seek (GstBaseSrc * src, GstSegment * segment)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "do_seek");

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

/* Clear any pending unlock request, as we succeeded in unlocking */
static gboolean
gst_rtmp2_src_unlock_stop (GstBaseSrc * src)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "unlock_stop");

  return TRUE;
}

/* notify subclasses of a query */
static gboolean
gst_rtmp2_src_query (GstBaseSrc * src, GstQuery * query)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "query");

  return TRUE;
}

/* notify subclasses of an event */
static gboolean
gst_rtmp2_src_event (GstBaseSrc * src, GstEvent * event)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "event");

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

/* ask the subclass to allocate an output buffer. The default implementation
 * will use the negotiated allocator. */
static GstFlowReturn
gst_rtmp2_src_alloc (GstBaseSrc * src, guint64 offset, guint size,
    GstBuffer ** buf)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "alloc");

  return GST_FLOW_OK;
}


/* URL handler */

static GstURIType
gst_rtmp2_src_uri_get_type (GType type)
{
  return GST_URI_SRC;
}

static const gchar *const *
gst_rtmp2_src_uri_get_protocols (GType type)
{
  static const gchar *protocols[] = { "rtmp", NULL };

  return protocols;
}

static gchar *
gst_rtmp2_src_uri_get_uri (GstURIHandler * handler)
{
  GstRtmp2Src *src = GST_RTMP2_SRC (handler);

  return gst_rtmp2_src_get_uri (src);
}

static gboolean
gst_rtmp2_src_uri_set_uri (GstURIHandler * handler, const gchar * uri,
    GError ** error)
{
  GstRtmp2Src *src = GST_RTMP2_SRC (handler);
  gboolean ret;

  ret = gst_rtmp2_src_set_uri (src, uri);
  if (!ret && error) {
    *error =
        g_error_new (GST_RESOURCE_ERROR, GST_RESOURCE_ERROR_FAILED,
        "Invalid URI");
  }

  return ret;
}


/* internal API */

static gchar *
gst_rtmp2_src_get_uri (GstRtmp2Src * src)
{
  if (src->port == 1935) {
    return g_strdup_printf ("rtmp://%s/%s/%s", src->server_address,
        src->application, src->stream);
  } else {
    return g_strdup_printf ("rtmp://%s:%d/%s/%s", src->server_address,
        src->port, src->application, src->stream);
  }
}

/* It would really be awesome if GStreamer had full URI parsing.  Alas. */
/* FIXME this function needs more error checking, and testing */
static gboolean
gst_rtmp2_src_set_uri (GstRtmp2Src * src, const char *uri)
{
  gchar *location;
  gchar **parts;
  gchar **parts2;
  gboolean ret;

  GST_DEBUG ("setting uri to %s", uri);

  if (!gst_uri_has_protocol (uri, "rtmp"))
    return FALSE;

  location = gst_uri_get_location (uri);

  parts = g_strsplit (location, "/", 3);
  if (parts[0] == NULL || parts[1] == NULL || parts[2] == NULL) {
    ret = FALSE;
    goto out;
  }

  parts2 = g_strsplit (parts[0], ":", 2);
  if (parts2[1]) {
    src->port = g_ascii_strtoull (parts2[1], NULL, 10);
  } else {
    src->port = 1935;
  }
  g_free (src->server_address);
  src->server_address = g_strdup (parts2[0]);
  g_strfreev (parts2);

  g_free (src->application);
  src->application = g_strdup (parts[1]);
  g_free (src->stream);
  src->stream = g_strdup (parts[2]);

  ret = TRUE;

out:
  g_free (location);
  g_strfreev (parts);
  return ret;
}

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
