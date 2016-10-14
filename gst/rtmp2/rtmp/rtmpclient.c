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
#include <gio/gio.h>
#include <string.h>
#include "rtmpclient.h"

GST_DEBUG_CATEGORY_STATIC (gst_rtmp_client_debug_category);
#define GST_CAT_DEFAULT gst_rtmp_client_debug_category

/* prototypes */

static void gst_rtmp_client_set_property (GObject * object,
    guint property_id, const GValue * value, GParamSpec * pspec);
static void gst_rtmp_client_get_property (GObject * object,
    guint property_id, GValue * value, GParamSpec * pspec);
static void gst_rtmp_client_dispose (GObject * object);
static void gst_rtmp_client_finalize (GObject * object);

static void
gst_rtmp_client_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);

enum
{
  PROP_0,
  PROP_SERVER_ADDRESS,
  PROP_SERVER_PORT,
  PROP_TIMEOUT
};

#define DEFAULT_SERVER_ADDRESS ""
#define DEFAULT_SERVER_PORT 1935
#define DEFAULT_TIMEOUT 5

/* pad templates */


/* class initialization */

G_DEFINE_TYPE_WITH_CODE (GstRtmpClient, gst_rtmp_client, G_TYPE_OBJECT,
    GST_DEBUG_CATEGORY_INIT (gst_rtmp_client_debug_category, "rtmpclient", 0,
        "debug category for GstRtmpClient class"));

static void
gst_rtmp_client_class_init (GstRtmpClientClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);

  gobject_class->set_property = gst_rtmp_client_set_property;
  gobject_class->get_property = gst_rtmp_client_get_property;
  gobject_class->dispose = gst_rtmp_client_dispose;
  gobject_class->finalize = gst_rtmp_client_finalize;

  g_object_class_install_property (gobject_class, PROP_SERVER_ADDRESS,
      g_param_spec_string ("server-address", "RTMP Server Address",
          "Address of RTMP server",
          DEFAULT_SERVER_ADDRESS, G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_SERVER_PORT,
      g_param_spec_int ("port", "RTMP server port",
          "RTMP server port (usually 1935)",
          1, 65535, DEFAULT_SERVER_PORT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));
  g_object_class_install_property (gobject_class, PROP_TIMEOUT,
      g_param_spec_int ("timeout", "Socket timeout",
          "Socket timeout, in seconds", 0, 1000, DEFAULT_TIMEOUT,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

}

static void
gst_rtmp_client_init (GstRtmpClient * rtmpclient)
{
  rtmpclient->server_address = g_strdup (DEFAULT_SERVER_ADDRESS);
  rtmpclient->server_port = DEFAULT_SERVER_PORT;

  rtmpclient->connection = gst_rtmp_connection_new ();
}

void
gst_rtmp_client_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstRtmpClient *rtmpclient = GST_RTMP_CLIENT (object);

  GST_DEBUG_OBJECT (rtmpclient, "set_property");

  switch (property_id) {
    case PROP_SERVER_ADDRESS:
      gst_rtmp_client_set_server_address (rtmpclient,
          g_value_get_string (value));
      break;
    case PROP_SERVER_PORT:
      gst_rtmp_client_set_server_port (rtmpclient, g_value_get_int (value));
      break;
    case PROP_TIMEOUT:
      rtmpclient->timeout = g_value_get_int (value);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp_client_get_property (GObject * object, guint property_id,
    GValue * value, GParamSpec * pspec)
{
  GstRtmpClient *rtmpclient = GST_RTMP_CLIENT (object);

  GST_DEBUG_OBJECT (rtmpclient, "get_property");

  switch (property_id) {
    case PROP_SERVER_ADDRESS:
      g_value_set_string (value, rtmpclient->server_address);
      break;
    case PROP_SERVER_PORT:
      g_value_set_int (value, rtmpclient->server_port);
      break;
    case PROP_TIMEOUT:
      g_value_set_int (value, rtmpclient->timeout);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

void
gst_rtmp_client_dispose (GObject * object)
{
  GstRtmpClient *rtmpclient = GST_RTMP_CLIENT (object);

  GST_DEBUG_OBJECT (rtmpclient, "dispose");

  /* clean up as possible.  may be called multiple times */

  G_OBJECT_CLASS (gst_rtmp_client_parent_class)->dispose (object);
}

void
gst_rtmp_client_finalize (GObject * object)
{
  GstRtmpClient *rtmpclient = GST_RTMP_CLIENT (object);

  GST_DEBUG_OBJECT (rtmpclient, "finalize");

  /* clean up object here */
  g_free (rtmpclient->server_address);
  g_free (rtmpclient->stream);
  g_clear_object (&rtmpclient->connection);

  G_OBJECT_CLASS (gst_rtmp_client_parent_class)->finalize (object);
}

/* API */

GstRtmpClient *
gst_rtmp_client_new (void)
{

  return g_object_new (GST_TYPE_RTMP_CLIENT, NULL);

}

void
gst_rtmp_client_set_server_address (GstRtmpClient * client,
    const char *server_address)
{
  g_free (client->server_address);
  client->server_address = g_strdup (server_address);
}

void
gst_rtmp_client_set_server_port (GstRtmpClient * client, int port)
{
  client->server_port = port;
}

void
gst_rtmp_client_set_stream (GstRtmpClient * client, const char *stream)
{
  g_free (client->stream);
  client->stream = g_strdup (stream);
}

void
gst_rtmp_client_connect_async (GstRtmpClient * client,
    GCancellable * cancellable, GAsyncReadyCallback callback,
    gpointer user_data)
{
  GSocketClient *socket_client;
  GTask *task;
  GSocketConnectable *addr;

  task = g_task_new (client, cancellable, callback, user_data);

  if (client->state != GST_RTMP_CLIENT_STATE_NEW) {
    g_task_return_new_error (task, GST_RTMP_ERROR,
        GST_RTMP_ERROR_TOO_LAZY, "already connected");
    g_object_unref (task);
    return;
  }

  addr = g_network_address_new (client->server_address, client->server_port);
  socket_client = g_socket_client_new ();
  g_socket_client_set_timeout (socket_client, client->timeout);

  GST_DEBUG ("g_socket_client_connect_async");
  g_socket_client_connect_async (socket_client, addr, cancellable,
      gst_rtmp_client_connect_done, task);
  g_object_unref (addr);
}

static void
gst_rtmp_client_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data)
{
  GSocketClient *socket_client = G_SOCKET_CLIENT (source);
  GTask *task = user_data;
  GstRtmpClient *client = g_task_get_source_object (task);
  GError *error = NULL;

  GST_DEBUG ("g_socket_client_connect_done");
  client->socket_connection =
      g_socket_client_connect_finish (socket_client, result, &error);
  g_object_unref (socket_client);
  if (client->socket_connection == NULL) {
    GST_ERROR ("error");
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  gst_rtmp_connection_set_socket_connection (client->connection,
      client->socket_connection);
  gst_rtmp_connection_start_handshake (client->connection, FALSE);

  g_task_return_boolean (task, TRUE);
  g_object_unref (task);
}

gboolean
gst_rtmp_client_connect_finish (GstRtmpClient * client,
    GAsyncResult * result, GError ** error)
{
  GTask *task = G_TASK (result);
  g_return_val_if_fail (g_task_is_valid (task, client), FALSE);
  return g_task_propagate_boolean (task, error);
}

GstRtmpConnection *
gst_rtmp_client_get_connection (GstRtmpClient * client)
{
  return client->connection;
}
