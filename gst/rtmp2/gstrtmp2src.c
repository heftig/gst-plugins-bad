/* GStreamer
 * Copyright (C) 2014 David Schleef <ds@schleef.org>
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

#include "gstrtmp2locationhandler.h"
#include "rtmp/rtmpchunk.h"
#include "rtmp/rtmpclient.h"
#include "rtmp/rtmputils.h"

#include <gst/base/gstpushsrc.h>
#include <string.h>

GST_DEBUG_CATEGORY_STATIC (gst_rtmp2_src_debug_category);
#define GST_CAT_DEFAULT gst_rtmp2_src_debug_category

/* prototypes */
#define GST_RTMP2_SRC(obj)   (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_RTMP2_SRC,GstRtmp2Src))
#define GST_RTMP2_SRC_CLASS(klass)   (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_RTMP2_SRC,GstRtmp2SrcClass))
#define GST_IS_RTMP2_SRC(obj)   (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_RTMP2_SRC))
#define GST_IS_RTMP2_SRC_CLASS(obj)   (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_RTMP2_SRC))
typedef struct _GstRtmp2Src GstRtmp2Src;
typedef struct _GstRtmp2SrcClass GstRtmp2SrcClass;

struct _GstRtmp2Src
{
  GstPushSrc base_rtmp2src;

  /* properties */
  GstRtmpLocation location;

  /* stuff */
  gboolean sent_header;
  GMutex lock;
  GCond cond;
  GstRtmpChunk *chunk;
  gboolean flushing;
  GstTask *task;
  GRecMutex task_lock;
  GMainLoop *task_main_loop;

  GTask *connect_task;
  GstRtmpConnection *connection;
};

struct _GstRtmp2SrcClass
{
  GstPushSrcClass base_rtmp2src_class;
};

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
static gboolean gst_rtmp2_src_unlock_stop (GstBaseSrc * src);
static GstFlowReturn gst_rtmp2_src_create (GstBaseSrc * src, guint64 offset,
    guint size, GstBuffer ** buf);

/* Internal API */
static void gst_rtmp2_src_task (gpointer user_data);
static void client_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static void send_create_stream (GTask * task);
static void create_stream_done (const gchar * command_name, GPtrArray * args,
    gpointer user_data);
static void send_play (GTask * task);
static void play_done (const gchar * command_name, GPtrArray * args,
    gpointer user_data);
static void new_connect (GstRtmp2Src * rtmp2src);
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
    G_IMPLEMENT_INTERFACE (GST_TYPE_RTMP_LOCATION_HANDLER, NULL);)

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
  base_src_class->unlock_stop = GST_DEBUG_FUNCPTR (gst_rtmp2_src_unlock_stop);
  base_src_class->create = GST_DEBUG_FUNCPTR (gst_rtmp2_src_create);

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
}

static void
gst_rtmp2_src_uri_handler_init (GstURIHandlerInterface * iface)
{
  gst_rtmp_location_handler_implement_uri_handler (iface, GST_URI_SRC);
}

void
gst_rtmp2_src_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstRtmp2Src *self = GST_RTMP2_SRC (object);

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
gst_rtmp2_src_get_property (GObject * object, guint property_id,
    GValue * value, GParamSpec * pspec)
{
  GstRtmp2Src *self = GST_RTMP2_SRC (object);

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
gst_rtmp2_src_finalize (GObject * object)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (object);

  /* clean up object here */
  g_clear_object (&rtmp2src->connect_task);
  g_clear_object (&rtmp2src->connection);
  gst_rtmp_location_clear (&rtmp2src->location);
  g_clear_object (&rtmp2src->task);
  g_rec_mutex_clear (&rtmp2src->task_lock);
  g_mutex_clear (&rtmp2src->lock);
  g_cond_clear (&rtmp2src->cond);
  g_clear_pointer (&rtmp2src->chunk, gst_rtmp_chunk_free);

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

static gboolean
gst_rtmp2_src_stop (GstBaseSrc * src)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "stop");

  g_mutex_lock (&rtmp2src->lock);
  if (rtmp2src->connect_task) {
    g_cancellable_cancel (g_task_get_cancellable (rtmp2src->connect_task));
  }
  gst_task_stop (rtmp2src->task);
  if (rtmp2src->task_main_loop) {
    g_main_loop_quit (rtmp2src->task_main_loop);
  }
  g_mutex_unlock (&rtmp2src->lock);

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
  rtmp2src->flushing = TRUE;
  g_cond_broadcast (&rtmp2src->cond);
  g_mutex_unlock (&rtmp2src->lock);

  return TRUE;
}

static gboolean
gst_rtmp2_src_unlock_stop (GstBaseSrc * src)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (src);

  GST_DEBUG_OBJECT (rtmp2src, "unlock_stop");

  g_mutex_lock (&rtmp2src->lock);
  rtmp2src->flushing = FALSE;
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
  const guint8 *payload_data;
  gsize payload_size;
  guint8 *buf_data, *buf_ptr;

  static const guint8 header[] = {
    0x46, 0x4c, 0x56, 0x01, 0x01, 0x00, 0x00, 0x00,
    0x09, 0x00, 0x00, 0x00, 0x00,
  };

  GST_LOG_OBJECT (rtmp2src, "create");

  g_mutex_lock (&rtmp2src->lock);
  while (!rtmp2src->chunk) {
    if (!rtmp2src->task_main_loop) {
      g_mutex_unlock (&rtmp2src->lock);
      return GST_FLOW_EOS;
    }
    if (rtmp2src->flushing) {
      g_mutex_unlock (&rtmp2src->lock);
      return GST_FLOW_FLUSHING;
    }
    g_cond_wait (&rtmp2src->cond, &rtmp2src->lock);
  }

  chunk = rtmp2src->chunk;
  rtmp2src->chunk = NULL;
  g_cond_signal (&rtmp2src->cond);
  g_mutex_unlock (&rtmp2src->lock);

  payload_data = g_bytes_get_data (chunk->payload, &payload_size);
  buf_data = buf_ptr = g_malloc (payload_size + 11 + 4 +
      (rtmp2src->sent_header ? 0 : sizeof header));

  if (!rtmp2src->sent_header) {
    rtmp2src->sent_header = TRUE;
    memcpy (buf_ptr, header, sizeof header);
    buf_ptr += sizeof header;
  }

  GST_WRITE_UINT8 (buf_ptr, chunk->message_type_id);
  GST_WRITE_UINT24_BE (buf_ptr + 1, chunk->message_length);
  GST_WRITE_UINT24_BE (buf_ptr + 4, chunk->timestamp);
  GST_WRITE_UINT8 (buf_ptr + 7, chunk->timestamp >> 24);
  GST_WRITE_UINT24_BE (buf_ptr + 8, 0);
  buf_ptr += 11;

  memcpy (buf_ptr, payload_data, payload_size);
  buf_ptr += payload_size;

  GST_WRITE_UINT32_BE (buf_ptr, payload_size + 11);
  buf_ptr += 4;
  gst_rtmp_chunk_free (chunk);

  *buf = gst_buffer_new_wrapped (buf_data, buf_ptr - buf_data);

  return GST_FLOW_OK;
}

/* Mainloop task */
static void
gst_rtmp2_src_task (gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);
  GMainContext *main_context;
  GMainLoop *main_loop;

  GST_DEBUG ("gst_rtmp2_src_task starting");

  main_context = g_main_context_new ();
  g_main_context_push_thread_default (main_context);

  g_mutex_lock (&rtmp2src->lock);
  main_loop = rtmp2src->task_main_loop = g_main_loop_new (main_context, TRUE);
  new_connect (rtmp2src);
  g_mutex_unlock (&rtmp2src->lock);

  g_main_loop_run (main_loop);

  g_mutex_lock (&rtmp2src->lock);
  g_clear_pointer (&rtmp2src->task_main_loop, g_main_loop_unref);
  g_clear_pointer (&rtmp2src->connection, gst_rtmp_connection_close_and_unref);
  g_cond_broadcast (&rtmp2src->cond);
  g_mutex_unlock (&rtmp2src->lock);

  while (g_main_context_pending (main_context)) {
    GST_DEBUG ("iterating main context to clean up");
    g_main_context_iteration (main_context, FALSE);
  }

  g_main_context_pop_thread_default (main_context);
  g_main_context_unref (main_context);

  g_mutex_lock (&rtmp2src->lock);
  g_clear_pointer (&rtmp2src->chunk, gst_rtmp_chunk_free);
  g_mutex_unlock (&rtmp2src->lock);

  GST_DEBUG ("gst_rtmp2_src_task exiting");
}

static void
new_connect (GstRtmp2Src * rtmp2src)
{
  GCancellable *cancellable = g_cancellable_new ();

  g_warn_if_fail (!rtmp2src->connect_task);
  rtmp2src->connect_task =
      g_task_new (rtmp2src, cancellable, connect_task_done, NULL);

  GST_OBJECT_LOCK (rtmp2src);
  gst_rtmp_client_connect_async (&rtmp2src->location,
      cancellable, client_connect_done, rtmp2src->connect_task);
  GST_OBJECT_UNLOCK (rtmp2src);

  g_object_unref (cancellable);
}

static void
got_chunk (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (user_data);

  g_return_if_fail (chunk);

  if (chunk->stream_id == 0) {
    goto uninteresting;
  }

  if (chunk->message_length == 0) {
    goto uninteresting;
  }

  switch (chunk->message_type_id) {
    case GST_RTMP_MESSAGE_TYPE_VIDEO:
    case GST_RTMP_MESSAGE_TYPE_AUDIO:
    case GST_RTMP_MESSAGE_TYPE_DATA:
      break;

    default:
      goto uninteresting;
  }

  g_mutex_lock (&rtmp2src->lock);
  while (rtmp2src->chunk) {
    if (rtmp2src->flushing) {
      g_mutex_unlock (&rtmp2src->lock);
      gst_rtmp_chunk_free (chunk);
      return;
    }
    g_cond_wait (&rtmp2src->cond, &rtmp2src->lock);
  }

  rtmp2src->chunk = chunk;
  g_cond_signal (&rtmp2src->cond);
  g_mutex_unlock (&rtmp2src->lock);
  return;

uninteresting:
  GST_DEBUG_OBJECT (rtmp2src,
      "not interested in chunk type %d stream %d size %" G_GSIZE_FORMAT,
      chunk->message_type_id, chunk->stream_id, chunk->message_length);
  gst_rtmp_chunk_free (chunk);
}

static void
connection_closed (GstRtmpConnection * connection, GstRtmp2Src * rtmp2src)
{
  g_mutex_lock (&rtmp2src->lock);
  if (rtmp2src->connect_task) {
    g_cancellable_cancel (g_task_get_cancellable (rtmp2src->connect_task));
  } else if (rtmp2src->task_main_loop) {
    GST_INFO_OBJECT (rtmp2src, "Connection got closed");
    gst_task_stop (rtmp2src->task);
    g_main_loop_quit (rtmp2src->task_main_loop);
  }
  g_mutex_unlock (&rtmp2src->lock);
}

static void
connect_task_done (GObject * object, GAsyncResult * result, gpointer user_data)
{
  GstRtmp2Src *rtmp2src = GST_RTMP2_SRC (object);
  GTask *task = G_TASK (result);
  GError *error = NULL;

  g_mutex_lock (&rtmp2src->lock);

  g_warn_if_fail (rtmp2src->connect_task == task);
  g_warn_if_fail (g_task_is_valid (task, object));

  rtmp2src->connect_task = NULL;
  rtmp2src->connection = g_task_propagate_pointer (task, &error);
  if (rtmp2src->connection) {
    gst_rtmp_connection_set_input_handler (rtmp2src->connection,
        got_chunk, g_object_ref (rtmp2src), g_object_unref);
    g_signal_connect_object (rtmp2src->connection, "closed",
        G_CALLBACK (connection_closed), rtmp2src, 0);
  } else {
    if (g_error_matches (error, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED)) {
      GST_ELEMENT_ERROR (rtmp2src, RESOURCE, NOT_AUTHORIZED,
          ("Not authorized to play from server"),
          ("%s", error ? GST_STR_NULL (error->message) : "(NULL error)"));
    } else if (g_error_matches (error, G_IO_ERROR,
            G_IO_ERROR_CONNECTION_REFUSED)) {
      GST_ELEMENT_ERROR (rtmp2src, RESOURCE, OPEN_READ,
          ("Could not connect to server"), ("%s",
              error ? GST_STR_NULL (error->message) : "(NULL error)"));
    } else if (!g_error_matches (error, G_IO_ERROR, G_IO_ERROR_CANCELLED)) {
      GST_ELEMENT_ERROR (rtmp2src, RESOURCE, FAILED,
          ("Could not connect to server"),
          ("%s", error ? GST_STR_NULL (error->message) : "(NULL error)"));
    }
  }

  g_cond_broadcast (&rtmp2src->cond);
  g_mutex_unlock (&rtmp2src->lock);
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
  GstAmfNode *node;

  node = gst_amf_node_new_null ();
  gst_rtmp_connection_send_command (connection, create_stream_done, task, 0,
      "createStream", node, NULL);
  gst_amf_node_free (node);
}

static void
create_stream_done (const gchar * command_name, GPtrArray * args,
    gpointer user_data)
{
  GTask *task = G_TASK (user_data);
  GstRtmp2Src *rtmp2src = g_task_get_source_object (task);

  if (args->len < 2) {
    g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
        "createStream failed");
    g_object_unref (task);
    return;
  }

  GST_DEBUG_OBJECT (rtmp2src, "createStream success, stream_id=%.0f",
      gst_amf_node_get_number (g_ptr_array_index (args, 1)));

  if (g_task_return_error_if_cancelled (task)) {
    g_object_unref (task);
    return;
  }

  send_play (task);
}

static void
send_play (GTask * task)
{
  GstRtmpConnection *connection = g_task_get_task_data (task);
  GstRtmp2Src *rtmp2src = g_task_get_source_object (task);
  GstAmfNode *node;
  GstAmfNode *node2;
  GstAmfNode *node3;

  gst_rtmp_connection_expect_command (connection, play_done, task, 1,
      "onStatus");

  node = gst_amf_node_new_null ();
  node3 = gst_amf_node_new_number (0);

  GST_OBJECT_LOCK (rtmp2src);
  node2 = gst_amf_node_new_string (rtmp2src->location.stream);
  GST_OBJECT_UNLOCK (rtmp2src);

  gst_rtmp_connection_send_command (connection, NULL, NULL, 1,
      "play", node, node2, node3, NULL);

  gst_amf_node_free (node);
  gst_amf_node_free (node2);
  gst_amf_node_free (node3);
}

static void
play_done (const gchar * command_name, GPtrArray * args, gpointer user_data)
{
  GTask *task = G_TASK (user_data);
  GstRtmpConnection *connection = g_task_get_task_data (task);
  GstRtmp2Src *rtmp2src = g_task_get_source_object (task);
  GstAmfType optional_args_type = GST_AMF_TYPE_INVALID;
  GstAmfNode *optional_args;

  if (args->len > 1) {
    optional_args = g_ptr_array_index (args, 1);
    optional_args_type = gst_amf_node_get_type (optional_args);
  }

  switch (optional_args_type) {
    case GST_AMF_TYPE_OBJECT:{
      const GstAmfNode *node = gst_amf_node_get_field (optional_args, "code");
      const gchar *code = node ? gst_amf_node_peek_string (node) : NULL;

      if (g_str_equal (code, "NetStream.Play.Start") ||
          g_str_equal (code, "NetStream.Play.Reset")) {
        GST_DEBUG_OBJECT (rtmp2src, "play success, code=%s", code);
        g_task_return_pointer (task, g_object_ref (connection),
            gst_rtmp_connection_close_and_unref);
      } else {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
            "unhandled play result code: %s", GST_STR_NULL (code));
      }
      break;
    }

    default:
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
          "play failed");
      break;
  }

  g_task_set_task_data (task, NULL, NULL);
  g_object_unref (task);
}
