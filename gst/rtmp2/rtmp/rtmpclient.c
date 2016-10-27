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
#include "rtmputils.h"

GST_DEBUG_CATEGORY_STATIC (gst_rtmp_client_debug_category);
#define GST_CAT_DEFAULT gst_rtmp_client_debug_category

static void socket_connect (GTask * task);
static void socket_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data);
static gchar *do_adobe_auth (const gchar * username, const gchar * password,
    const gchar * salt, const gchar * opaque, const gchar * challenge);
static void send_connect (GTask * task, GstRtmpConnection * connection);
static void send_connect_done (GstRtmpConnection * connection,
    GstRtmpChunk * chunk, const char *command_name, int transaction_id,
    GstAmfNode * command_object, GstAmfNode * optional_args,
    gpointer user_data);
static void send_secure_token_response (GTask * task,
    GstRtmpConnection * connection, const gchar * challenge);

void
gst_rtmp_location_copy (GstRtmpLocation * dest, const GstRtmpLocation * src)
{
  g_return_if_fail (dest);
  g_return_if_fail (src);

  dest->host = g_strdup (src->host);
  dest->port = src->port;
  dest->application = g_strdup (src->application);
  dest->stream = g_strdup (src->stream);
  dest->username = g_strdup (src->username);
  dest->password = g_strdup (src->password);
  dest->secure_token = g_strdup (src->secure_token);
  dest->authmod = src->authmod;
  dest->timeout = src->timeout;
}

void
gst_rtmp_location_clear (GstRtmpLocation * location)
{
  g_return_if_fail (location);

  g_clear_pointer (&location->host, g_free);
  location->port = 0;
  g_clear_pointer (&location->application, g_free);
  g_clear_pointer (&location->stream, g_free);
  g_clear_pointer (&location->username, g_free);
  g_clear_pointer (&location->password, g_free);
  g_clear_pointer (&location->secure_token, g_free);
}

gchar *
gst_rtmp_location_get_string (const GstRtmpLocation * uri, gboolean with_stream)
{
  GstUri *gsturi;
  gchar *string;

  gsturi = gst_uri_new ("rtmp", NULL, uri->host,
      uri->port == GST_RTMP_DEFAULT_PORT ? GST_URI_NO_PORT : uri->port, "/",
      NULL, NULL);

  gst_uri_append_path_segment (gsturi, uri->application);

  if (with_stream) {
    gst_uri_append_path_segment (gsturi, uri->stream);
  }

  string = gst_uri_to_string (gsturi);
  gst_uri_unref (gsturi);

  return string;
}

#define DEFAULT_TIMEOUT 5

typedef struct
{
  GstRtmpLocation location;
  gchar *auth_query;
} TaskData;

static TaskData *
task_data_new (const GstRtmpLocation * location)
{
  TaskData *data = g_slice_new (TaskData);
  data->auth_query = NULL;
  gst_rtmp_location_copy (&data->location, location);
  return data;
}

static void
task_data_free (gpointer ptr)
{
  TaskData *data = ptr;
  gst_rtmp_location_clear (&data->location);
  g_clear_pointer (&data->auth_query, g_free);
  g_slice_free (TaskData, data);
}

static GRegex *auth_regex = NULL;

GType
gst_rtmp_authmod_get_type (void)
{
  static volatile gsize authmod_type = 0;
  static const GEnumValue authmod[] = {
    {GST_RTMP_AUTHMOD_NONE, "GST_RTMP_AUTHMOD_NONE", "none"},
    {GST_RTMP_AUTHMOD_AUTO, "GST_RTMP_AUTHMOD_AUTO", "auto"},
    {GST_RTMP_AUTHMOD_ADOBE, "GST_RTMP_AUTHMOD_ADOBE", "adobe"},
    {0, NULL, NULL},
  };

  if (g_once_init_enter (&authmod_type)) {
    GType tmp = g_enum_register_static ("GstRtmpAuthmod", authmod);
    g_once_init_leave (&authmod_type, tmp);
  }

  return (GType) authmod_type;
}

void
gst_rtmp_client_connect_async (const GstRtmpLocation * location,
    GCancellable * cancellable, GAsyncReadyCallback callback,
    gpointer user_data)
{
  GTask *task;

  if (g_once_init_enter (&auth_regex)) {
    GRegex *re = g_regex_new ("\\[ *AccessManager.Reject *\\] *: *"
        "\\[ *authmod=adobe *\\] *: *"
        "\\?reason=needauth"
        "&user=(?<user>.*?)"
        "&salt=(?<salt>.*?)"
        "&challenge=(?<challenge>.*?)"
        "&opaque=(?<opaque>.*?)\\Z", G_REGEX_DOTALL, 0, NULL);
    g_warn_if_fail (re);

    GST_DEBUG_CATEGORY_INIT (gst_rtmp_client_debug_category,
        "rtmpclient", 0, "debug category for rtmpclient");

    g_once_init_leave (&auth_regex, re);
  }

  task = g_task_new (NULL, cancellable, callback, user_data);

  g_task_set_task_data (task, task_data_new (location), task_data_free);

  socket_connect (task);
}

static void
socket_connect (GTask * task)
{
  TaskData *data = g_task_get_task_data (task);
  GSocketConnectable *addr;
  GSocketClient *socket_client;

  if (data->location.timeout < 0) {
    data->location.timeout = DEFAULT_TIMEOUT;
  }

  addr = g_network_address_new (data->location.host, data->location.port);
  socket_client = g_socket_client_new ();
  g_socket_client_set_timeout (socket_client, data->location.timeout);

  GST_DEBUG ("g_socket_client_connect_async");
  g_socket_client_connect_async (socket_client, addr,
      g_task_get_cancellable (task), socket_connect_done, task);
  g_object_unref (addr);
}

static void
socket_connect_done (GObject * source, GAsyncResult * result,
    gpointer user_data)
{
  GSocketClient *socket_client = G_SOCKET_CLIENT (source);
  GTask *task = user_data;
  GError *error = NULL;
  GSocketConnection *socket_connection;
  GstRtmpConnection *rtmp_connection;

  GST_DEBUG ("g_socket_client_connect_done");
  socket_connection =
      g_socket_client_connect_finish (socket_client, result, &error);
  g_object_unref (socket_client);

  if (g_task_return_error_if_cancelled (task)) {
    GST_DEBUG ("was cancelled");
    g_object_unref (task);
    return;
  }

  if (socket_connection == NULL) {
    GST_ERROR ("error");
    g_task_return_error (task, error);
    g_object_unref (task);
    return;
  }

  rtmp_connection = gst_rtmp_connection_new (socket_connection);
  gst_rtmp_connection_start_handshake (rtmp_connection, FALSE);
  g_object_unref (socket_connection);

  send_connect (task, rtmp_connection);
  g_object_unref (rtmp_connection);
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
send_connect (GTask * task, GstRtmpConnection * connection)
{
  TaskData *data = g_task_get_task_data (task);
  GstAmfNode *node;
  const gchar *app;
  gchar *uri, *appstr = NULL, *uristr = NULL;

  node = gst_amf_node_new (GST_AMF_TYPE_OBJECT);
  app = data->location.application;
  uri = gst_rtmp_location_get_string (&data->location, FALSE);

  if (data->auth_query) {
    const gchar *query = data->auth_query;
    appstr = g_strdup_printf ("%s?%s", app, query);
    uristr = g_strdup_printf ("%s?%s", uri, query);
  } else if (data->location.authmod == GST_RTMP_AUTHMOD_ADOBE) {
    const gchar *user = data->location.username;
    const gchar *authmod = "adobe";
    appstr = g_strdup_printf ("%s?authmod=%s&user=%s", app, authmod, user);
    uristr = g_strdup_printf ("%s?authmod=%s&user=%s", uri, authmod, user);
  } else {
    appstr = g_strdup (app);
    uristr = g_strdup (uri);
  }

  gst_amf_object_set_string_take (node, "app", appstr);
  gst_amf_object_set_string_take (node, "tcUrl", uristr);
  gst_amf_object_set_string (node, "type", "nonprivate");
  gst_amf_object_set_string (node, "flashVer", "FMLE/3.0");

  // "fpad": False,
  // "capabilities": 15,
  // "audioCodecs": 3191,
  // "videoCodecs": 252,
  // "videoFunction": 1,

  gst_rtmp_connection_send_command (connection, 3, "connect", 1,
      node, NULL, send_connect_done, task);

  gst_amf_node_free (node);
  g_free (uri);
}

static void
send_connect_done (GstRtmpConnection * connection, GstRtmpChunk * chunk,
    const char *command_name, int transaction_id, GstAmfNode * command_object,
    GstAmfNode * optional_args, gpointer user_data)
{
  GTask *task = G_TASK (user_data);
  TaskData *data = g_task_get_task_data (task);
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
  GST_INFO ("connect result: %s", GST_STR_NULL (code));

  if (g_str_equal (code, "NetConnection.Connect.Success")) {
    node = gst_amf_node_get_object (optional_args, "secureToken");
    send_secure_token_response (task, connection,
        node ? gst_amf_node_get_string (node) : NULL);
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
    GST_DEBUG ("connect result desc: %s", GST_STR_NULL (desc));

    if (g_strrstr (desc, "code=403 need auth; authmod=adobe")) {
      GST_INFO ("server requires adobe style auth mode");

      if (!(data->location.authmod == GST_RTMP_AUTHMOD_AUTO ||
              data->location.authmod == GST_RTMP_AUTHMOD_ADOBE)) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "adobe style auth required, but incompatible auth mode selected");
        g_object_unref (task);
        return;
      }
      if (!data->location.username || !data->location.username[0] ||
          !data->location.password || !data->location.password[0]) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
            "adobe style auth required, but no credentials supplied");
        g_object_unref (task);
        return;
      }

      data->location.authmod = GST_RTMP_AUTHMOD_ADOBE;
      gst_rtmp_connection_close (connection);
      socket_connect (task);
      return;
    }

    if (g_strrstr (desc, "?reason=authfailed")) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "authentication failed! wrong credentials?");
      g_object_unref (task);
      return;
    }

    if (g_regex_match (auth_regex, desc, 0, &match_info)) {
      const gchar *username = data->location.username;
      const gchar *password = data->location.password;
      gchar *salt = g_match_info_fetch_named (match_info, "salt");
      gchar *challenge = g_match_info_fetch_named (match_info, "challenge");
      gchar *opaque = g_match_info_fetch_named (match_info, "opaque");

      g_match_info_free (match_info);

      GST_INFO
          ("regex parsed auth... user='%s', salt='%s', challenge='%s', opaque='%s'",
          GST_STR_NULL (data->location.username), GST_STR_NULL (salt),
          GST_STR_NULL (challenge), GST_STR_NULL (opaque));

      g_warn_if_fail (data->location.authmod == GST_RTMP_AUTHMOD_ADOBE);
      data->location.authmod = GST_RTMP_AUTHMOD_ADOBE;

      g_warn_if_fail (!data->auth_query);
      data->auth_query =
          do_adobe_auth (username, password, salt, opaque, challenge);
      g_free (salt);
      g_free (opaque);
      g_free (challenge);

      if (!data->auth_query) {
        g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_FAILED,
            "couldn't generate adobe style authentication query");
        g_object_unref (task);
        return;
      }

      gst_rtmp_connection_close (connection);
      socket_connect (task);
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
send_secure_token_response (GTask * task, GstRtmpConnection * connection,
    const gchar * challenge)
{
  if (challenge) {
    TaskData *data = g_task_get_task_data (task);
    GstAmfNode *node1;
    GstAmfNode *node2;
    gchar *response;

    if (!data->location.secure_token || !data->location.secure_token[0]) {
      g_task_return_new_error (task, G_IO_ERROR, G_IO_ERROR_PERMISSION_DENIED,
          "server requires secure token authentication");
      g_object_unref (task);
      return;
    }

    response = gst_rtmp_tea_decode (data->location.secure_token, challenge);

    GST_DEBUG ("response: %s", response);

    node1 = gst_amf_node_new (GST_AMF_TYPE_NULL);
    node2 = gst_amf_node_new (GST_AMF_TYPE_STRING);
    gst_amf_node_set_string_take (node2, response);

    gst_rtmp_connection_send_command (connection, 3,
        "secureTokenResponse", 0, node1, node2, NULL, NULL);
    gst_amf_node_free (node1);
    gst_amf_node_free (node2);
  }

  g_task_return_pointer (task, g_object_ref (connection),
      gst_rtmp_connection_close_and_unref);
  g_object_unref (task);
}

GstRtmpConnection *
gst_rtmp_client_connect_finish (GAsyncResult * result, GError ** error)
{
  GTask *task = G_TASK (result);
  return g_task_propagate_pointer (task, error);
}
