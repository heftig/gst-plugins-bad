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
static void connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void send_connect (GTask * task);
static void cmd_connect_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
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
static void send_secure_token_response (GTask * task, const char *challenge);

typedef struct
{
  GstRtmp2URI uri;
  GstRtmpConnection *connection;
  GstRtmpAuthmod authmod;       /* current state */
  gchar *auth_query;
} GstRtmpConnectData;

static GstRtmpConnectData *
gst_rtmp_connect_data_new (GstRtmp2URI * uri)
{
  GstRtmpConnectData *data;

  g_return_val_if_fail (uri, NULL);

  data = g_slice_new0 (GstRtmpConnectData);
  gst_rtmp2_uri_copy (&data->uri, uri);
  return data;
}

static void
rtmp_connection_close_and_unref (gpointer ptr)
{
  GstRtmpConnection *connection;

  g_return_if_fail (ptr);

  connection = GST_RTMP_CONNECTION (ptr);
  gst_rtmp_connection_close (connection);
  g_object_unref (connection);
}

static void
gst_rtmp_connect_data_free (gpointer ptr)
{
  GstRtmpConnectData *data = ptr;

  g_return_if_fail (data);

  g_clear_pointer (&data->connection, rtmp_connection_close_and_unref);
  gst_rtmp2_uri_clear (&data->uri);
  g_free (data->auth_query);
  g_slice_free (GstRtmpConnectData, data);
}

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
  PROP_AUTH_METHOD
};

#define DEFAULT_TIMEOUT 5
#define DEFAULT_PUBLISHING_TYPE "live"
#define DEFAULT_SECURE_TOKEN ""
#define DEFAULT_AUTH_METHOD  GST_RTMP_AUTHMOD_AUTO

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
    G_IMPLEMENT_INTERFACE (GST_TYPE_RTMP2_URI_HANDLER, NULL);)

     static GRegex *auth_regex;

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

  g_object_class_install_property (gobject_class, PROP_SECURE_TOKEN,
      g_param_spec_string ("secure-token", "Secure token",
          "Secure token used for authentication",
          DEFAULT_SECURE_TOKEN,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  g_object_class_override_property (gobject_class, PROP_USERNAME, "username");
  g_object_class_override_property (gobject_class, PROP_PASSWORD, "password");

  g_object_class_install_property (gobject_class, PROP_AUTH_METHOD,
      g_param_spec_enum ("auth", "Auth Method",
          "Select the authorization method",
          GST_TYPE_RTMP2_AUTH_METHOD, DEFAULT_AUTH_METHOD,
          G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  GST_DEBUG_CATEGORY_INIT (gst_rtmp2_sink_debug_category, "rtmp2sink", 0,
      "debug category for rtmp2sink element");

  auth_regex = g_regex_new ("\\[ *AccessManager.Reject *\\] *: *"
      "\\[ *authmod=adobe *\\] *: *"
      "\\?reason=needauth"
      "&user=(?<user>.*?)"
      "&salt=(?<salt>.*?)"
      "&challenge=(?<challenge>.*?)"
      "&opaque=(?<opaque>.*?)\\Z", G_REGEX_DOTALL, 0, NULL);
  g_warn_if_fail (auth_regex);
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
  gst_rtmp2_uri_handler_implement_uri_handler (iface, GST_URI_SINK);
}

GType
gst_rtmp2_auth_method_get_type (void)
{
  static volatile gsize auth_method_type = 0;
  static const GEnumValue auth_method[] = {
    {GST_RTMP_AUTHMOD_NONE, "GST_RTMP_AUTHMOD_NONE", "none"},
    {GST_RTMP_AUTHMOD_AUTO, "GST_RTMP_AUTHMOD_AUTO", "auto"},
    {GST_RTMP_AUTHMOD_ADOBE, "GST_RTMP_AUTHMOD_ADOBE", "adobe"},
    {0, NULL, NULL},
  };

  if (g_once_init_enter (&auth_method_type)) {
    GType tmp = g_enum_register_static ("GstRtmpAuthmod", auth_method);
    g_once_init_leave (&auth_method_type, tmp);
  }
  return (GType) auth_method_type;
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
    case PROP_AUTH_METHOD:
    {
      GstRtmpAuthmod mode = g_value_get_enum (value);
      GEnumValue *val =
          g_enum_get_value (G_ENUM_CLASS (g_type_class_ref
              (GST_TYPE_RTMP2_AUTH_METHOD)), mode);
      const gchar *value_nick = val->value_nick;
      GST_OBJECT_LOCK (self);
      if (self->auth_method != mode) {
        self->auth_method = mode;
        GST_INFO_OBJECT (self, "successfully set auth method to %s (%i)",
            value_nick, mode);
      }
      GST_OBJECT_UNLOCK (self);
      break;
    }
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
    case PROP_AUTH_METHOD:
      GST_OBJECT_LOCK (self);
      g_value_set_enum (value, self->auth_method);
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
  gst_rtmp2_uri_clear (&rtmp2sink->uri);
  g_free (rtmp2sink->secure_token);
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
  g_main_loop_quit (rtmp2sink->task_main_loop);
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

  gst_rtmp_connection_queue_chunk (rtmp2sink->connection, chunk);

  return GST_FLOW_OK;
}

/* Internal API */

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
  if (!rtmp2sink->connection) {
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
do_connect (GTask * task)
{
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  GstRtmpClient *client = gst_rtmp_client_new ();
  g_object_set (client, "timeout", DEFAULT_TIMEOUT, NULL);

  gst_rtmp_client_set_server_address (client, connect_data->uri.host);
  gst_rtmp_client_set_server_port (client, connect_data->uri.port);
  gst_rtmp_client_connect_async (client, g_task_get_cancellable (task),
      connect_done, task);
}

static void
new_connect (GstRtmp2Sink * rtmp2sink)
{
  GstRtmpConnectData *connect_data =
      gst_rtmp_connect_data_new (&rtmp2sink->uri);
  GCancellable *cancellable = g_cancellable_new ();

  rtmp2sink->connect_task =
      g_task_new (rtmp2sink, cancellable, connect_task_done, NULL);
  g_task_set_task_data (rtmp2sink->connect_task, connect_data,
      gst_rtmp_connect_data_free);

  do_connect (rtmp2sink->connect_task);
}

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

  g_clear_pointer (&rtmp2sink->connection, rtmp_connection_close_and_unref);

  while (g_main_context_pending (main_context)) {
    GST_DEBUG ("iterating main context to clean up");
    g_main_context_iteration (main_context, FALSE);
  }

  g_main_context_pop_thread_default (main_context);
  g_main_context_unref (main_context);

  GST_DEBUG ("gst_rtmp2_sink_task exiting");
}

static void
connect_done (GObject * source, GAsyncResult * result, gpointer user_data)
{
  GstRtmpClient *client = GST_RTMP_CLIENT (source);
  GTask *task = user_data;
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  GError *error = NULL;
  gboolean ret;

  ret = gst_rtmp_client_connect_finish (client, result, &error);
  if (!ret) {
    g_task_return_error (task, error);
    g_object_unref (client);
    g_object_unref (task);
    return;
  }

  /* saved here so it gets closed by gst_rtmp_connect_data_free
   * when the task errors */
  g_clear_pointer (&connect_data->connection, rtmp_connection_close_and_unref);
  connect_data->connection =
      g_object_ref (gst_rtmp_client_get_connection (client));
  g_object_unref (client);

  if (g_task_return_error_if_cancelled (task)) {
    g_object_unref (task);
    return;
  }

  send_connect (task);
}

static gchar *
do_adobe_auth (const gchar * username, const gchar * password,
    const gchar * salt, const gchar * opaque, const gchar * challenge)
{
  guint8 hash[16];              /* MD5 digest */
  gsize hashlen = sizeof hash;
  gchar *challenge2, *auth_query;
  GChecksum *md5;

  md5 = g_checksum_new (G_CHECKSUM_MD5);
  g_checksum_update (md5, (guchar *) username, -1);
  g_checksum_update (md5, (guchar *) salt, -1);
  g_checksum_update (md5, (guchar *) password, -1);

  g_checksum_get_digest (md5, hash, &hashlen);
  g_warn_if_fail (hashlen == sizeof hash);

  {
    gchar *hashstr = g_base64_encode ((guchar *) hash, sizeof hash);
    g_checksum_reset (md5);
    g_checksum_update (md5, (guchar *) hashstr, -1);
    g_free (hashstr);
  }

  if (opaque)
    g_checksum_update (md5, (guchar *) opaque, -1);
  else if (challenge)
    g_checksum_update (md5, (guchar *) challenge, -1);

  challenge2 = g_strdup_printf ("%08x", g_random_int ());
  g_checksum_update (md5, (guchar *) challenge2, -1);

  g_checksum_get_digest (md5, hash, &hashlen);
  g_warn_if_fail (hashlen == sizeof hash);

  {
    gchar *hashstr = g_base64_encode ((guchar *) hash, sizeof hash);

    if (opaque) {
      auth_query =
          g_strdup_printf
          ("authmod=%s&user=%s&challenge=%s&response=%s&opaque=%s", "adobe",
          username, challenge2, hashstr, opaque);
    } else {
      auth_query =
          g_strdup_printf ("authmod=%s&user=%s&challenge=%s&response=%s",
          "adobe", username, challenge2, hashstr);
    }
    g_free (hashstr);
  }

  g_checksum_free (md5);
  g_free (challenge2);

  return auth_query;
}

static void
send_connect (GTask * task)
{
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  GstAmfNode *node;
  const gchar *app;
  gchar *uri;

  node = gst_amf_node_new (GST_AMF_TYPE_OBJECT);
  app = connect_data->uri.application;
  uri = gst_rtmp2_uri_get_string (&connect_data->uri, FALSE);

  if (connect_data->authmod > GST_RTMP_AUTHMOD_NONE) {
    gchar *appstr, *uristr;

    if (connect_data->auth_query) {
      const gchar *query = connect_data->auth_query;
      appstr = g_strdup_printf ("%s?%s", app, query);
      uristr = g_strdup_printf ("%s?%s", uri, query);
    } else {
      const gchar *user = connect_data->uri.username;
      const gchar *authmod = g_strdup ("adobe");
      appstr = g_strdup_printf ("%s?authmod=%s&user=%s", app, authmod, user);
      uristr = g_strdup_printf ("%s?authmod=%s&user=%s", uri, authmod, user);
    }

    gst_amf_object_set_string (node, "app", appstr);
    gst_amf_object_set_string (node, "tcUrl", uristr);

    g_free (appstr);
    g_free (uristr);
  } else {
    gst_amf_object_set_string (node, "app", app);
    gst_amf_object_set_string (node, "tcUrl", uri);
  }

  gst_amf_object_set_string (node, "type", "nonprivate");
  gst_amf_object_set_string (node, "flashVer", "FMLE/3.0");

  // "fpad": False,
  // "capabilities": 15,
  // "audioCodecs": 3191,
  // "videoCodecs": 252,
  // "videoFunction": 1,

  gst_rtmp_connection_send_command (connect_data->connection, 3, "connect", 1,
      node, NULL, cmd_connect_done, task);

  gst_amf_node_free (node);
  g_free (uri);
}

static void
cmd_connect_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GTask *task = G_TASK (user_data);
  GstRtmp2Sink *rtmp2sink = g_task_get_source_object (task);
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  const GstAmfNode *node;
  const gchar *code;

  if (!optional_args) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
        "arguments missing from connect cmd result");
    g_object_unref (task);
    return;
  }

  node = gst_amf_node_get_object (optional_args, "code");
  if (!node) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
        "result code missing from connect cmd result");
    g_object_unref (task);
    return;
  }

  code = gst_amf_node_get_string (node);
  GST_INFO_OBJECT (rtmp2sink, "connect result: %s", GST_STR_NULL (code));

  if (g_str_equal (code, "NetConnection.Connect.Success")) {
    node = gst_amf_node_get_object (optional_args, "secureToken");
    if (node) {
      send_secure_token_response (task, gst_amf_node_get_string (node));
    } else {
      send_create_stream (task);
    }
    return;
  }

  if (g_str_equal (code, "NetConnection.Connect.Rejected")) {
    GMatchInfo *match_info;
    const gchar *desc;

    node = gst_amf_node_get_object (optional_args, "description");
    if (!node) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "Connect rejected; no description");
      g_object_unref (task);
      return;
    }

    desc = gst_amf_node_get_string (node);
    if (g_strrstr (desc, "code=403 need auth; authmod=adobe")) {
      GST_INFO_OBJECT (rtmp2sink, "server requires adobe style auth mode");

      if (!(rtmp2sink->auth_method == GST_RTMP_AUTHMOD_AUTO ||
              rtmp2sink->auth_method == GST_RTMP_AUTHMOD_ADOBE)) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "adobe style auth required, but incompatible auth method %s selected",
            g_enum_get_value (G_ENUM_CLASS (g_type_class_ref
                    (GST_TYPE_RTMP2_AUTH_METHOD)),
                rtmp2sink->auth_method)->value_nick);
        g_object_unref (task);
        return;
      }
      if (!connect_data->uri.username || !connect_data->uri.username[0] ||
          !connect_data->uri.password || !connect_data->uri.password[0]) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "adobe style auth required, but no credentials supplied");
        g_object_unref (task);
        return;
      }

      connect_data->authmod = GST_RTMP_AUTHMOD_ADOBE;
      do_connect (task);
      return;
    }

    if (g_strrstr (desc, "?reason=authfailed")) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "authentication failed! wrong credentials?");
      g_object_unref (task);
      return;
    }

    if (g_regex_match (auth_regex, desc, 0, &match_info)) {
      const gchar *username = connect_data->uri.username;
      const gchar *password = connect_data->uri.password;
      gchar *salt = g_match_info_fetch_named (match_info, "salt");
      gchar *challenge = g_match_info_fetch_named (match_info, "challenge");
      gchar *opaque = g_match_info_fetch_named (match_info, "opaque");

      g_match_info_free (match_info);

      GST_INFO_OBJECT (rtmp2sink,
          "regex parsed auth... user='%s', salt='%s', challenge='%s', opaque='%s'",
          GST_STR_NULL (connect_data->uri.username), GST_STR_NULL (salt),
          GST_STR_NULL (challenge), GST_STR_NULL (opaque));

      g_warn_if_fail (connect_data->authmod == GST_RTMP_AUTHMOD_ADOBE);
      connect_data->authmod = GST_RTMP_AUTHMOD_ADOBE;
      connect_data->auth_query =
          do_adobe_auth (username, password, salt, opaque, challenge);
      g_free (salt);
      g_free (opaque);
      g_free (challenge);

      if (!connect_data->auth_query) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
            "couldn't generate adobe style authentication query");
        g_object_unref (task);
        return;
      }

      do_connect (task);
      return;
    }

    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
        "unhandled auth rejection: %s", GST_STR_NULL (desc));
    g_object_unref (task);
    return;
  }

  g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
      "unhandled connect result code: %s", GST_STR_NULL (code));
  g_object_unref (task);
}

static void
send_create_stream (GTask * task)
{
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  GstAmfNode *node;
  GstAmfNode *node2;

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, connect_data->uri.stream);
  gst_rtmp_connection_send_command (connect_data->connection, 3,
      "releaseStream", 2, node, node2, NULL, NULL);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, connect_data->uri.stream);
  gst_rtmp_connection_send_command (connect_data->connection, 3, "FCPublish", 3,
      node, node2, NULL, NULL);
  gst_amf_node_free (node);
  gst_amf_node_free (node2);

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  gst_rtmp_connection_send_command (connect_data->connection, 3, "createStream",
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

  send_publish (task);
}

static void
send_publish (GTask * task)
{
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  GstAmfNode *node;
  GstAmfNode *node2;
  GstAmfNode *node3;

  node = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node2, connect_data->uri.stream);
  node3 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string (node3, DEFAULT_PUBLISHING_TYPE);
  gst_rtmp_connection_send_command2 (connect_data->connection, 4, 1, "publish",
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
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  const GstAmfNode *node;
  const gchar *code = NULL;

  if (!optional_args) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
        "publish failed");
    g_object_unref (task);
    return;
  }

  node = gst_amf_node_get_object (optional_args, "code");
  if (node) {
    code = gst_amf_node_get_string (node);
  }

  GST_DEBUG_OBJECT (rtmp2sink, "publish return, stream_id=%.0f, code=%s",
      gst_amf_node_get_number (optional_args), GST_STR_NULL (code));

  if (code) {
    if (g_str_equal (code, "NetStream.Publish.Denied")) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "Publish denied! (%s)", code);
    } else {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
          "unhandled publish result code: %s", code);
    }
    g_object_unref (task);
    return;
  }

  /* Steal and return connection pointer */
  g_task_return_pointer (task, connect_data->connection,
      rtmp_connection_close_and_unref);
  connect_data->connection = NULL;
  g_object_unref (task);
}

static void
send_secure_token_response (GTask * task, const char *challenge)
{
  GstRtmp2Sink *rtmp2sink = g_task_get_source_object (task);
  GstRtmpConnectData *connect_data = g_task_get_task_data (task);
  GstAmfNode *node1;
  GstAmfNode *node2;
  gchar *response;

  if (!rtmp2sink->secure_token || !rtmp2sink->secure_token[0]) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
        "server requires secure token authentication");
    g_object_unref (task);
    return;
  }

  response = gst_rtmp_tea_decode (rtmp2sink->secure_token, challenge);

  GST_DEBUG_OBJECT (rtmp2sink, "response: %s", response);

  node1 = gst_amf_node_new (GST_AMF_TYPE_NULL);
  node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
  gst_amf_node_set_string_take (node2, response);

  gst_rtmp_connection_send_command (connect_data->connection, 3,
      "secureTokenResponse", 0, node1, node2, NULL, NULL);
  gst_amf_node_free (node1);
  gst_amf_node_free (node2);

  send_create_stream (task);
}
