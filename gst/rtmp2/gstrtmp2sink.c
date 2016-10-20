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
static gboolean gst_rtmp2_sink_unlock_stop (GstBaseSink * sink);
static GstFlowReturn gst_rtmp2_sink_preroll (GstBaseSink * sink,
    GstBuffer * buffer);
static GstFlowReturn gst_rtmp2_sink_render (GstBaseSink * sink,
    GstBuffer * buffer);

/* Internal API */
static void gst_rtmp2_sink_task (gpointer user_data);
static void client_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void send_create_stream (GTask * task);
static void create_stream_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void send_publish (GTask * task);
static void publish_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void new_connect (GstRtmp2Sink * rtmp2sink);
static void connect_task_done (GObject * object, GAsyncResult * result,
    gpointer user_data);


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
  PROP_AUTHMOD,
  PROP_TIMEOUT,
};

#define DEFAULT_PUBLISHING_TYPE "live"

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
    G_IMPLEMENT_INTERFACE (GST_TYPE_RTMP_LOCATION_HANDLER, NULL);
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
  base_sink_class->unlock_stop = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_unlock_stop);
  base_sink_class->preroll = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_preroll);
  base_sink_class->render = GST_DEBUG_FUNCPTR (gst_rtmp2_sink_render);

  g_object_class_override_property (gobject_class, PROP_LOCATION, "location");
  g_object_class_override_property (gobject_class, PROP_HOST, "host");
  g_object_class_override_property (gobject_class, PROP_PORT, "port");
  g_object_class_override_property (gobject_class, PROP_APPLICATION,
      "application");
  g_object_class_override_property (gobject_class, PROP_STREAM, "stream");
  g_object_class_override_property (gobject_class, PROP_SECURE_TOKEN,
      "secure-token");
  g_object_class_override_property (gobject_class, PROP_USERNAME, "username");
  g_object_class_override_property (gobject_class, PROP_PASSWORD, "password");
  g_object_class_override_property (gobject_class, PROP_AUTHMOD, "authmod");
  g_object_class_override_property (gobject_class, PROP_TIMEOUT, "timeout");

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
}

static void
gst_rtmp2_sink_uri_handler_init (GstURIHandlerInterface * iface)
{
  gst_rtmp_location_handler_implement_uri_handler (iface, GST_URI_SINK);
}

void
gst_rtmp2_sink_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstRtmp2Sink *self = GST_RTMP2_SINK (object);

  switch (property_id) {
    case PROP_LOCATION:
      gst_rtmp_location_handler_set_uri (GST_RTMP_LOCATION_HANDLER (self),
          g_value_get_string (value));
      break;
    case PROP_HOST:
      GST_OBJECT_LOCK (self);
      g_free (self->location.host);
      self->location.host = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PORT:
      GST_OBJECT_LOCK (self);
      self->location.port = g_value_get_int (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_APPLICATION:
      GST_OBJECT_LOCK (self);
      g_free (self->location.application);
      self->location.application = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_STREAM:
      GST_OBJECT_LOCK (self);
      g_free (self->location.stream);
      self->location.stream = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_SECURE_TOKEN:
      GST_OBJECT_LOCK (self);
      g_free (self->location.secure_token);
      self->location.secure_token = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_USERNAME:
      GST_OBJECT_LOCK (self);
      g_free (self->location.username);
      self->location.username = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PASSWORD:
      GST_OBJECT_LOCK (self);
      g_free (self->location.password);
      self->location.password = g_value_dup_string (value);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_AUTHMOD:
    {
      GstRtmpAuthmod mode = g_value_get_enum (value);
      GST_OBJECT_LOCK (self);
      if (self->location.authmod != mode) {
        self->location.authmod = mode;
        GST_INFO_OBJECT (self, "successfully set auth method to (%i)", mode);
      }
      GST_OBJECT_UNLOCK (self);
      break;
    }
    case PROP_TIMEOUT:
      GST_OBJECT_LOCK (self);
      self->location.timeout = g_value_get_uint (value);
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
      g_value_take_string (value, gst_rtmp_location_get_string (&self->location,
              TRUE));
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_HOST:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->location.host);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PORT:
      GST_OBJECT_LOCK (self);
      g_value_set_int (value, self->location.port);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_APPLICATION:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->location.application);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_STREAM:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->location.stream);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_SECURE_TOKEN:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->location.secure_token);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_USERNAME:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->location.username);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_PASSWORD:
      GST_OBJECT_LOCK (self);
      g_value_set_string (value, self->location.password);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_AUTHMOD:
      GST_OBJECT_LOCK (self);
      g_value_set_enum (value, self->location.authmod);
      GST_OBJECT_UNLOCK (self);
      break;
    case PROP_TIMEOUT:
      GST_OBJECT_LOCK (self);
      g_value_set_uint (value, self->location.timeout);
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
  g_clear_object (&rtmp2sink->connect_task);
  g_clear_object (&rtmp2sink->connection);
  gst_rtmp_location_clear (&rtmp2sink->location);
  g_clear_object (&rtmp2sink->task);
  g_rec_mutex_clear (&rtmp2sink->task_lock);
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

  g_mutex_lock (&rtmp2sink->lock);
  if (rtmp2sink->connect_task) {
    g_cancellable_cancel (g_task_get_cancellable (rtmp2sink->connect_task));
  }
  gst_task_stop (rtmp2sink->task);
  if (rtmp2sink->task_main_loop) {
    g_main_loop_quit (rtmp2sink->task_main_loop);
  }
  g_mutex_unlock (&rtmp2sink->lock);

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
  rtmp2sink->flushing = TRUE;
  g_cond_signal (&rtmp2sink->cond);
  g_mutex_unlock (&rtmp2sink->lock);

  return TRUE;
}

static gboolean
gst_rtmp2_sink_unlock_stop (GstBaseSink * sink)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);

  GST_DEBUG_OBJECT (rtmp2sink, "unlock_stop");

  g_mutex_lock (&rtmp2sink->lock);
  rtmp2sink->flushing = FALSE;
  g_mutex_unlock (&rtmp2sink->lock);

  return TRUE;
}

static GstFlowReturn
gst_rtmp2_sink_preroll (GstBaseSink * sink, GstBuffer * buffer)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);

  g_mutex_lock (&rtmp2sink->lock);
  while (!rtmp2sink->flushing && !rtmp2sink->connection) {
    g_cond_wait (&rtmp2sink->cond, &rtmp2sink->lock);
  }
  g_mutex_unlock (&rtmp2sink->lock);

  return rtmp2sink->connection ? GST_FLOW_OK : GST_FLOW_FLUSHING;
}

static GstFlowReturn
gst_rtmp2_sink_render (GstBaseSink * sink, GstBuffer * buffer)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (sink);
  GstRtmpChunk *chunk;
  GBytes *bytes;
  gsize size;
  guint8 *data;

  GST_LOG_OBJECT (rtmp2sink, "render");

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

  g_mutex_lock (&rtmp2sink->lock);
  if (rtmp2sink->connection) {
    gst_rtmp_connection_queue_chunk (rtmp2sink->connection, chunk);
    g_mutex_unlock (&rtmp2sink->lock);
    return GST_FLOW_OK;
  } else {
    g_mutex_unlock (&rtmp2sink->lock);
    g_object_unref (chunk);
    GST_DEBUG_OBJECT (rtmp2sink, "connection missing");
    return GST_FLOW_ERROR;
  }
}

/* Mainloop task */
static void
gst_rtmp2_sink_task (gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (user_data);
  GMainContext *main_context;

  GST_DEBUG ("gst_rtmp2_sink_task starting");

  main_context = g_main_context_new ();
  g_main_context_push_thread_default (main_context);

  rtmp2sink->task_main_loop = g_main_loop_new (main_context, TRUE);

  new_connect (rtmp2sink);

  g_main_loop_run (rtmp2sink->task_main_loop);
  g_clear_pointer (&rtmp2sink->task_main_loop, g_main_loop_unref);

  g_clear_pointer (&rtmp2sink->connection, gst_rtmp_connection_close_and_unref);

  while (g_main_context_pending (main_context)) {
    GST_DEBUG ("iterating main context to clean up");
    g_main_context_iteration (main_context, FALSE);
  }

  g_main_context_pop_thread_default (main_context);
  g_main_context_unref (main_context);

  GST_DEBUG ("gst_rtmp2_sink_task exiting");
}

static void
new_connect (GstRtmp2Sink * rtmp2sink)
{
  GCancellable *cancellable = g_cancellable_new ();

  g_warn_if_fail (!rtmp2sink->connect_task);
  rtmp2sink->connect_task =
      g_task_new (rtmp2sink, cancellable, connect_task_done, NULL);

  gst_rtmp_client_connect_async (&rtmp2sink->location,
      cancellable, client_connect_done, rtmp2sink->connect_task);

  g_object_unref (cancellable);
}

static void
connection_closed (GstRtmpConnection * connection, GstRtmp2Sink * rtmp2sink)
{
  g_mutex_lock (&rtmp2sink->lock);
  if (rtmp2sink->connect_task) {
    g_cancellable_cancel (g_task_get_cancellable (rtmp2sink->connect_task));
  } else if (rtmp2sink->task_main_loop) {
    GST_ELEMENT_ERROR (rtmp2sink, RESOURCE, WRITE,
        ("Connection got closed"), (NULL));
    gst_task_stop (rtmp2sink->task);
    g_main_loop_quit (rtmp2sink->task_main_loop);
  }
  g_mutex_unlock (&rtmp2sink->lock);
}

static void
connect_task_done (GObject * object, GAsyncResult * result, gpointer user_data)
{
  GstRtmp2Sink *rtmp2sink = GST_RTMP2_SINK (object);
  GTask *task = G_TASK (result);
  GError *error = NULL;

  g_mutex_lock (&rtmp2sink->lock);

  g_warn_if_fail (rtmp2sink->connect_task == task);
  g_warn_if_fail (g_task_is_valid (task, object));

  rtmp2sink->connect_task = NULL;
  rtmp2sink->connection = g_task_propagate_pointer (task, &error);
  if (rtmp2sink->connection) {
    g_signal_connect_object (rtmp2sink->connection, "closed",
        G_CALLBACK (connection_closed), rtmp2sink, 0);
  } else {
    if (g_error_matches (error, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED)) {
      GST_ELEMENT_ERROR (rtmp2sink, RESOURCE, NOT_AUTHORIZED,
          ("Not authorized to push to server"),
          ("%s", error ? GST_STR_NULL (error->message) : "(NULL error)"));
    } else if (g_error_matches (error, G_IO_ERROR,
            G_IO_ERROR_CONNECTION_REFUSED)) {
      GST_ELEMENT_ERROR (rtmp2sink, RESOURCE, OPEN_READ,
          ("Could not connect to server"), ("%s",
              error ? GST_STR_NULL (error->message) : "(NULL error)"));
    } else if (!g_error_matches (error, G_IO_ERROR, G_IO_ERROR_CANCELLED)) {
      GST_ELEMENT_ERROR (rtmp2sink, RESOURCE, FAILED,
          ("Could not connect to server"),
          ("%s", error ? GST_STR_NULL (error->message) : "(NULL error)"));
    }
  }

  g_cond_signal (&rtmp2sink->cond);
  g_mutex_unlock (&rtmp2sink->lock);
}

static void
client_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data)
{
  GTask *task = user_data;
  GError *error = NULL;
  GstRtmpConnection *connection;

  connection = gst_rtmp_client_connect_finish (result, &error);
  if (!connection) {
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  g_task_set_task_data (task, connection, g_object_unref);

  if (g_task_return_error_if_cancelled (task)) {
    g_object_unref (task);
    return;
  }

  send_create_stream (task);
}

static void
send_create_stream (GTask * task)
{
  GstRtmpConnection *connection = g_task_get_task_data (task);
  GstRtmp2Sink *rtmp2sink = g_task_get_source_object (task);
  GstAmfNode *node;
  GstAmfNode *node2;

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, rtmp2sink->location.stream);
  gst_rtmp_connection_send_command (connection, 3,
      "releaseStream", 2, node, node2, NULL, NULL);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, rtmp2sink->location.stream);
  gst_rtmp_connection_send_command (connection, 3, "FCPublish", 3,
      node, node2, NULL, NULL);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  gst_rtmp_connection_send_command (connection, 3, "createStream",
      4, node, NULL, create_stream_done, task);
  gst_amf_node_free (node);
}

static void
create_stream_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GTask *task = G_TASK (user_data);
  GstRtmp2Sink *rtmp2sink = g_task_get_source_object (task);

  if (!optional_args) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
        "createStream failed");
    g_object_unref (task);
    return;
  }

  GST_DEBUG_OBJECT (rtmp2sink, "createStream success, stream_id=%.0f",
      gst_amf_node_get_number (optional_args));

  if (g_task_return_error_if_cancelled (task)) {
    g_object_unref (task);
    return;
  }

  send_publish (task);
}

static void
send_publish (GTask * task)
{
  GstRtmpConnection *connection = g_task_get_task_data (task);
  GstRtmp2Sink *rtmp2sink = g_task_get_source_object (task);
  GstAmfNode *node;
  GstAmfNode *node2;
  GstAmfNode *node3;

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, rtmp2sink->location.stream);
  node3 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node3, DEFAULT_PUBLISHING_TYPE);
  gst_rtmp_connection_send_command2 (connection, 4, 1, "publish",
      5, node, node2, node3, NULL, publish_done, task);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);
  gst_amf_node_free (node3);
}

static void
publish_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GTask *task = G_TASK (user_data);
  GstRtmp2Sink *rtmp2sink = g_task_get_source_object (task);

  switch (optional_args ? optional_args->type : GST_AMF_TYPE_INVALID) {
    case GST_AMF_TYPE_NUMBER:
      GST_DEBUG_OBJECT (rtmp2sink, "publish success, stream_id=%.0f",
          gst_amf_node_get_number (optional_args));
      g_task_return_pointer (task, g_object_ref (connection),
          gst_rtmp_connection_close_and_unref);
      break;

    case GST_AMF_TYPE_OBJECT:{
      const GstAmfNode *node = gst_amf_node_get_object (optional_args, "code");
      const gchar *code = node ? gst_amf_node_get_string (node) : NULL;

      if (g_str_equal (code, "NetStream.Publish.Denied")) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "Publish denied! (%s)", code);
      } else {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
            "unhandled publish result code: %s", GST_STR_NULL (code));
      }
      break;
    }

    default:
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
          "publish failed");
      break;
  }

  g_task_set_task_data (task, NULL, NULL);
  g_object_unref (task);
}
